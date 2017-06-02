/**
 *    Copyright (C) 2013-2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/query/stage_builder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/client.h"
#include "mongo/db/exec/and_hash.h"
#include "mongo/db/exec/and_sorted.h"
#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/count_scan.h"
#include "mongo/db/exec/distinct_scan.h"
#include "mongo/db/exec/ensure_sorted.h"
#include "mongo/db/exec/fetch.h"
#include "mongo/db/exec/geo_near.h"
#include "mongo/db/exec/index_scan.h"
#include "mongo/db/exec/keep_mutations.h"
#include "mongo/db/exec/limit.h"
#include "mongo/db/exec/merge_sort.h"
#include "mongo/db/exec/or.h"
#include "mongo/db/exec/projection.h"
#include "mongo/db/exec/shard_filter.h"
#include "mongo/db/exec/skip.h"
#include "mongo/db/exec/sort.h"
#include "mongo/db/exec/sort_key_generator.h"
#include "mongo/db/exec/text.h"
#include "mongo/db/index/fts_access_method.h"
#include "mongo/db/matcher/extensions_callback_real.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"

namespace mongo {

using std::unique_ptr;
using stdx::make_unique;

auto buildStages(OperationContext* opCtx,
                 Collection* collection,
                 const CanonicalQuery& cq,
                 const QuerySolution& qsol,
                 const QuerySolutionNode* root,
                 WorkingSet* ws) -> std::unique_ptr<PlanStage> {

    switch (root->getType()) {
    case STAGE_COLLSCAN: {
        const CollectionScanNode* csn = static_cast<const CollectionScanNode*>(root);
        CollectionScanParams params;
        params.collection = collection;
        params.tailable = csn->tailable;
        params.direction =
            (csn->direction == 1) ? CollectionScanParams::FORWARD : CollectionScanParams::BACKWARD;
        params.maxScan = csn->maxScan;
        return stdx::make_unique<CollectionScan>(opCtx, params, ws, csn->filter.get());
    }
    case STAGE_IXSCAN: {
        const IndexScanNode* ixn = static_cast<const IndexScanNode*>(root);

        if (!collection) {
            warning() << "Can't ixscan null namespace";
            return NULL;
        }

        IndexScanParams params;

        params.descriptor = collection->getIndexCatalog()->findIndexByName(opCtx, ixn->index.name);
        invariant(params.descriptor);

        params.bounds = ixn->bounds;
        params.direction = ixn->direction;
        params.maxScan = ixn->maxScan;
        params.addKeyMetadata = ixn->addKeyMetadata;
        return stdx::make_unique<IndexScan>(opCtx, params, ws, ixn->filter.get());
    }
    case STAGE_FETCH: {
        const FetchNode* fn = static_cast<const FetchNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, fn->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<FetchStage>(
                opCtx, ws, std::move(childStage), fn->filter.get(), collection);
    }
    case STAGE_SORT: {
        const SortNode* sn = static_cast<const SortNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, sn->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        SortStageParams params;
        params.collection = collection;
        params.pattern = sn->pattern;
        params.limit = sn->limit;
        return stdx::make_unique<SortStage>(opCtx, params, ws, std::move(childStage));
    }
    case STAGE_SORT_KEY_GENERATOR: {
        const SortKeyGeneratorNode* keyGenNode = static_cast<const SortKeyGeneratorNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, keyGenNode->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<SortKeyGeneratorStage>(opCtx,
                std::move(childStage), ws, keyGenNode->sortSpec, keyGenNode->queryObj, cq.getCollator());
    }
    case STAGE_PROJECTION: {
        const ProjectionNode* pn = static_cast<const ProjectionNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, pn->children[0], ws);
        if (!childStage) {
            return NULL;
        }

        ProjectionStageParams params(ExtensionsCallbackReal(opCtx, &collection->ns()));
        params.projObj = pn->projection;
        params.collator = cq.getCollator();

        // Stuff the right data into the params depending on what proj impl we use.
        if (ProjectionNode::DEFAULT == pn->projType) {
            params.fullExpression = pn->fullExpression;
            params.projImpl = ProjectionStageParams::NO_FAST_PATH;
        } else if (ProjectionNode::COVERED_ONE_INDEX == pn->projType) {
            params.projImpl = ProjectionStageParams::COVERED_ONE_INDEX;
            params.coveredKeyObj = pn->coveredKeyObj;
            invariant(!pn->coveredKeyObj.isEmpty());
        } else {
            invariant(ProjectionNode::SIMPLE_DOC == pn->projType);
            params.projImpl = ProjectionStageParams::SIMPLE_DOC;
        }

        return stdx::make_unique<ProjectionStage>(opCtx, params, ws, std::move(childStage));
    }
    case STAGE_LIMIT: {
        const LimitNode* ln = static_cast<const LimitNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, ln->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<LimitStage>(opCtx, ln->limit, ws, std::move(childStage));
    }
    case STAGE_SKIP: {
        const SkipNode* sn = static_cast<const SkipNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, sn->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<SkipStage>(opCtx, sn->skip, ws, std::move(childStage));
    }
    case STAGE_AND_HASH: {
        const AndHashNode* ahn = static_cast<const AndHashNode*>(root);
        auto ret = make_unique<AndHashStage>(opCtx, ws, collection);
        for (size_t i = 0; i < ahn->children.size(); ++i) {
            auto childStage = buildStages(opCtx, collection, cq, qsol, ahn->children[i], ws);
            if (!childStage) {
                return NULL;
            }
            ret->addChild(std::move(childStage));
        }
        return ret;
    }
    case STAGE_OR: {
        const OrNode* orn = static_cast<const OrNode*>(root);
        auto ret = make_unique<OrStage>(opCtx, ws, orn->dedup, orn->filter.get());
        for (size_t i = 0; i < orn->children.size(); ++i) {
            auto childStage = buildStages(opCtx, collection, cq, qsol, orn->children[i], ws);
            if (!childStage) {
                return NULL;
            }
            ret->addChild(std::move(childStage));
        }
        return ret;
    }
    case STAGE_AND_SORTED: {
        const AndSortedNode* asn = static_cast<const AndSortedNode*>(root);
        auto ret = make_unique<AndSortedStage>(opCtx, ws, collection);
        for (size_t i = 0; i < asn->children.size(); ++i) {
            auto childStage = buildStages(opCtx, collection, cq, qsol, asn->children[i], ws);
            if (!childStage) {
                return NULL;
            }
            ret->addChild(std::move(childStage));
        }
        return ret;
    }
    case STAGE_SORT_MERGE: {
        const MergeSortNode* msn = static_cast<const MergeSortNode*>(root);
        MergeSortStageParams params;
        params.dedup = msn->dedup;
        params.pattern = msn->sort;
        params.collator = cq.getCollator();
        auto ret = make_unique<MergeSortStage>(opCtx, params, ws, collection);
        for (size_t i = 0; i < msn->children.size(); ++i) {
            auto childStage = buildStages(opCtx, collection, cq, qsol, msn->children[i], ws);
            if (!childStage) {
                return NULL;
            }
            ret->addChild(std::move(childStage));
        }
        return ret;
    }
    case STAGE_GEO_NEAR_2D: {
        const GeoNear2DNode* node = static_cast<const GeoNear2DNode*>(root);

        GeoNearParams params;
        params.nearQuery = node->nq;
        params.baseBounds = node->baseBounds;
        params.filter = node->filter.get();
        params.addPointMeta = node->addPointMeta;
        params.addDistMeta = node->addDistMeta;

        IndexDescriptor* twoDIndex =
            collection->getIndexCatalog()->findIndexByName(opCtx, node->index.name);
        invariant(twoDIndex);

        return stdx::make_unique<GeoNear2DStage>(params, opCtx, ws, collection, twoDIndex);
    }
    case STAGE_GEO_NEAR_2DSPHERE: {
        const GeoNear2DSphereNode* node = static_cast<const GeoNear2DSphereNode*>(root);

        GeoNearParams params;
        params.nearQuery = node->nq;
        params.baseBounds = node->baseBounds;
        params.filter = node->filter.get();
        params.addPointMeta = node->addPointMeta;
        params.addDistMeta = node->addDistMeta;

        IndexDescriptor* s2Index =
            collection->getIndexCatalog()->findIndexByName(opCtx, node->index.name);
        invariant(s2Index);

        return stdx::make_unique<GeoNear2DSphereStage>(params, opCtx, ws, collection, s2Index);
    }
    case STAGE_TEXT: {
        const TextNode* node = static_cast<const TextNode*>(root);
        IndexDescriptor* desc =
            collection->getIndexCatalog()->findIndexByName(opCtx, node->index.name);
        invariant(desc);
        const FTSAccessMethod* fam =
            static_cast<FTSAccessMethod*>(collection->getIndexCatalog()->getIndex(desc));
        invariant(fam);

        TextStageParams params(fam->getSpec());
        params.index = desc;
        params.indexPrefix = node->indexPrefix;
        // We assume here that node->ftsQuery is an FTSQueryImpl, not an FTSQueryNoop. In practice,
        // this means that it is illegal to use the StageBuilder on a QuerySolution created by
        // planning a query that contains "no-op" expressions. TODO: make StageBuilder::build()
        // fail in this case (this improvement is being tracked by SERVER-21510).
        params.query = static_cast<FTSQueryImpl&>(*node->ftsQuery);
        return stdx::make_unique<TextStage>(opCtx, params, ws, node->filter.get());
    }
    case STAGE_SHARDING_FILTER: {
        const ShardingFilterNode* fn = static_cast<const ShardingFilterNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, fn->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<ShardFilterStage>(
            opCtx,
            CollectionShardingState::get(opCtx, collection->ns())->getMetadata(),
            ws,
            std::move(childStage));
    }
    case STAGE_KEEP_MUTATIONS: {
        const KeepMutationsNode* km = static_cast<const KeepMutationsNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, km->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<KeepMutationsStage>(
            opCtx, km->filter.get(), ws, std::move(childStage));
    }
    case STAGE_DISTINCT_SCAN: {
        const DistinctNode* dn = static_cast<const DistinctNode*>(root);

        if (NULL == collection) {
            warning() << "Can't distinct-scan null namespace";
            return NULL;
        }

        DistinctParams params;

        params.descriptor = collection->getIndexCatalog()->findIndexByName(opCtx, dn->index.name);
        invariant(params.descriptor);
        params.direction = dn->direction;
        params.bounds = dn->bounds;
        params.fieldNo = dn->fieldNo;
        return stdx::make_unique<DistinctScan>(opCtx, params, ws);
    }
    case STAGE_COUNT_SCAN: {
        const CountScanNode* csn = static_cast<const CountScanNode*>(root);

        if (NULL == collection) {
            warning() << "Can't fast-count null namespace (collection null)";
            return NULL;
        }

        CountScanParams params;

        params.descriptor = collection->getIndexCatalog()->findIndexByName(opCtx, csn->index.name);
        invariant(params.descriptor);
        params.startKey = csn->startKey;
        params.startKeyInclusive = csn->startKeyInclusive;
        params.endKey = csn->endKey;
        params.endKeyInclusive = csn->endKeyInclusive;

        return stdx::make_unique<CountScan>(opCtx, params, ws);
    }
    case STAGE_ENSURE_SORTED: {
        const EnsureSortedNode* esn = static_cast<const EnsureSortedNode*>(root);
        auto childStage = buildStages(opCtx, collection, cq, qsol, esn->children[0], ws);
        if (!childStage) {
            return NULL;
        }
        return stdx::make_unique<EnsureSortedStage>(opCtx, esn->pattern, ws, std::move(childStage));
    }
    case STAGE_COUNT:
    case STAGE_DELETE:
    case STAGE_NOTIFY_DELETE:
    case STAGE_EOF:
    case STAGE_GROUP:
    case STAGE_IDHACK:
    case STAGE_INDEX_ITERATOR:
    case STAGE_MULTI_ITERATOR:
    case STAGE_MULTI_PLAN:
    case STAGE_OPLOG_START:
    case STAGE_PIPELINE_PROXY:
    case STAGE_QUEUED_DATA:
    case STAGE_SUBPLAN:
    case STAGE_TEXT_OR:
    case STAGE_TEXT_MATCH:
    case STAGE_UNKNOWN:
    case STAGE_UPDATE:
    case STAGE_CACHED_PLAN: {
        mongoutils::str::stream ss;
        root->appendToString(&ss, 0);
        string nodeStr(ss);
        warning() << "Can't build exec tree for node " << nodeStr << endl;
    }
    }
    return NULL;
}

// static (this one is used for Cached and MultiPlanStage)
bool StageBuilder::build(OperationContext* opCtx,
                         Collection* collection,
                         const CanonicalQuery& cq,
                         const QuerySolution& solution,
                         WorkingSet* wsIn,
                         std::unique_ptr<PlanStage>* rootOut) {
    // Only QuerySolutions derived from queries parsed with context, or QuerySolutions derived from
    // queries that disallow extensions, can be properly executed. If the query does not have
    // $text/$where context (and $text/$where are allowed), then no attempt should be made to
    // execute the query.
    invariant(!cq.hasNoopExtensions());

    if (NULL == wsIn || NULL == rootOut) {
        return false;
    }
    QuerySolutionNode* solutionNode = solution.root.get();
    if (NULL == solutionNode) {
        return false;
    }
    return NULL != (*rootOut = buildStages(opCtx, collection, cq, solution, solutionNode, wsIn));
}

}  // namespace mongo
