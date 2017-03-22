/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/collection_range_deleter.h"

#include <algorithm>
#include <utility>

#include "mongo/db/catalog/collection.h"
#include "mongo/db/client.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/query_knobs.h"
#include "mongo/db/query/query_planner.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/metadata_manager.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/service_context.h"
#include "mongo/db/write_concern.h"
#include "mongo/executor/task_executor.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

class ChunkRange;
class OldClientWriteContext;

using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using logger::LogComponent;

namespace {

const WriteConcernOptions kMajorityWriteConcern(WriteConcernOptions::kMajority,
                                                WriteConcernOptions::SyncMode::UNSET,
                                                Seconds(60));

}  // unnamed namespace

CollectionRangeDeleter::~CollectionRangeDeleter() {
    clear();  // notify anybody sleeping on orphan ranges
}

bool CollectionRangeDeleter::cleanupNextRange(OperationContext* opCtx,
                                              CollectionRangeDeleter* self,
                                              NamespaceString const& nss,
                                              stdx::mutex* lock,
                                              int maxToDelete) {
    {
        AutoGetCollection autoColl(opCtx, nss, MODE_IX);

        auto* collection = autoColl.getCollection();
        if (!collection) {
            return false;
        }
        // Note: Outside this block, there is no expectation that *self or *lock still exist.

        auto* collectionShardingState = CollectionShardingState::get(opCtx, nss);
        boost::optional<const ChunkRange> nextRange = boost::none;
        {
            stdx::lock_guard<stdx::mutex> scopedLock(*lock);
            if (!self->isEmpty()) {
                auto& front = self->_orphans.front().range;
                nextRange.emplace(front.getMin(), front.getMax());
            }
        }
        if (!nextRange) {
            return false;
        }

        auto scopedCollectionMetadata = collectionShardingState->getMetadata();
        auto const keyPattern = scopedCollectionMetadata->getKeyPattern();
        auto countWith = self->_doDeletion(opCtx, collection, *nextRange, keyPattern, maxToDelete);
        if (countWith.getValue() == 0) {
            stdx::lock_guard<stdx::mutex> scopedLock(*lock);
            invariant(!self->isEmpty());
            self->_pop(countWith.getStatus());
            return true;
        }
    }

    // wait for replication
    WriteConcernResult wcResult;
    auto currentClientOpTime = repl::ReplClientInfo::forClient(opCtx->getClient()).getLastOp();
    Status status =
        waitForWriteConcern(opCtx, currentClientOpTime, kMajorityWriteConcern, &wcResult);
    if (!status.isOK()) {
        warning() << "Error when waiting for write concern after removing chunks in " << nss
                  << " : " << status.reason();
    }

    return true;
}

StatusWith<int> CollectionRangeDeleter::_doDeletion(OperationContext* opCtx,
                                                    Collection* collection,
                                                    ChunkRange const& range,
                                                    const BSONObj& keyPattern,
                                                    int maxToDelete) {
    invariant(collection);

    // The IndexChunk has a keyPattern that may apply to more than one index - we need to
    // select the index and get the full index keyPattern here.
    const IndexDescriptor* idx =
        collection->getIndexCatalog()->findShardKeyPrefixedIndex(opCtx, keyPattern, false);
    if (idx == NULL) {
        std::string msg = str::stream() << "Unable to find shard key index for "
                                        << keyPattern.toString() << " in " << collection->ns().ns();
        log() << msg;
        return {ErrorCodes::InternalError, msg};
    }

    KeyPattern indexKeyPattern(idx->keyPattern().getOwned());

    // Extend bounds to match the index we found
    const BSONObj& min =
        Helpers::toKeyFormat(indexKeyPattern.extendRangeBound(range.getMin(), false));
    const BSONObj& max =
        Helpers::toKeyFormat(indexKeyPattern.extendRangeBound(range.getMax(), false));

    LOG(1) << "begin removal of " << min << " to " << max << " in " << collection->ns().ns();

    auto indexName = idx->indexName();
    IndexDescriptor* desc = collection->getIndexCatalog()->findIndexByName(opCtx, indexName);
    if (!desc) {
        std::string msg = str::stream() << "shard key index with name " << indexName << " on '"
                                        << collection->ns().ns() << "' was dropped";
        log() << msg;
        return {ErrorCodes::InternalError, msg};
    }

    int numDeleted = 0;
    do {
        auto bounds = BoundInclusion::kIncludeStartKeyOnly;
        auto yield = PlanExecutor::YIELD_MANUAL;
        auto fwd = InternalPlanner::FORWARD;
        auto ix = InternalPlanner::IXSCAN_FETCH;
        auto exec =
            InternalPlanner::indexScan(opCtx, collection, desc, min, max, bounds, yield, fwd, ix);
        RecordId rloc;
        BSONObj obj;
        PlanExecutor::ExecState state = exec->getNext(&obj, &rloc);
        if (state == PlanExecutor::IS_EOF) {
            break;
        }
        if (state == PlanExecutor::FAILURE || state == PlanExecutor::DEAD) {
            warning(LogComponent::kSharding)
                << PlanExecutor::statestr(state) << " - cursor error while trying to delete " << min
                << " to " << max << " in " << collection->ns() << ": "
                << WorkingSetCommon::toStatusString(obj)
                << ", stats: " << Explain::getWinningPlanStats(exec.get());
            break;
        }

        invariant(PlanExecutor::ADVANCED == state);
        WriteUnitOfWork wuow(opCtx);
        if (!repl::getGlobalReplicationCoordinator()->canAcceptWritesFor(opCtx, collection->ns())) {
            warning() << "stepped down from primary while deleting chunk; orphaning data in "
                      << collection->ns() << " in range [" << min << ", " << max << ")";
            break;
        }
        OpDebug* const nullOpDebug = nullptr;
        collection->deleteDocument(opCtx, rloc, nullOpDebug, true);
        wuow.commit();
    } while (++numDeleted < maxToDelete);
    return numDeleted;
}

auto CollectionRangeDeleter::overlaps(ChunkRange const& range) -> DeleteNotification {
    // start search with newest entries by using reverse iterators
    auto it = find_if(_orphans.rbegin(), _orphans.rend(), [&](auto& cleanee) {
        return bool(cleanee.range.overlapWith(range));
    });
    return it != _orphans.rend() ? it->notification : DeleteNotification();
}

void CollectionRangeDeleter::add(ChunkRange const& range) {
    // We ignore the case of overlapping, or even equal, ranges.

    // Deleting overlapping ranges is similarly quick.
    _orphans.emplace_back(Deletion{ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()),
                                   std::make_shared<Notification<Status>>()});
}

void CollectionRangeDeleter::append(BSONObjBuilder* builder) const {
    BSONArrayBuilder arr(builder->subarrayStart("rangesToClean"));
    for (auto const& entry : _orphans) {
        BSONObjBuilder obj;
        entry.range.append(&obj);
        arr.append(obj.done());
    }
    arr.done();
}

size_t CollectionRangeDeleter::size() const {
    return _orphans.size();
}

bool CollectionRangeDeleter::isEmpty() const {
    return _orphans.empty();
}

void CollectionRangeDeleter::clear() {
    std::for_each(_orphans.begin(), _orphans.end(), [](auto& range) {
        // Since deletion was not actually tried, we have no failures to report.
        if (!*(range.notification)) {
            range.notification->set(Status::OK());  // wake up anything waiting on it
        }
    });
    _orphans.clear();
}

void CollectionRangeDeleter::_pop(Status result) {
    _orphans.front().notification->set(result);  // wake up waitForClean
    _orphans.pop_front();
}

}  // namespace mongo
