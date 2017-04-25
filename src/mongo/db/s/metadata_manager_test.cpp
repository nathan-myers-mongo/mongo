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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/s/metadata_manager.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/remote_command_targeter_mock.h"
#include "mongo/db/client.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/s/type_shard_identity.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/dist_lock_catalog_impl.h"
#include "mongo/s/catalog/dist_lock_manager_mock.h"
#include "mongo/s/catalog/sharding_catalog_client_mock.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/sharding_mongod_test_fixture.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"


#include <boost/optional.hpp>

namespace mongo {
namespace {

using unittest::assertGet;

const NamespaceString kNss("TestDB", "TestColl");
const std::string kPattern = "X";
const BSONObj kShardKeyPattern{BSON(kPattern << 1)};
const std::string kShardName{"a"};
const HostAndPort dummyHost("dummy", 123);

class MetadataManagerTest : public ShardingMongodTestFixture {
public:
    std::shared_ptr<RemoteCommandTargeterMock> configTargeter() {
        return RemoteCommandTargeterMock::get(shardRegistry()->getConfigShard()->getTargeter());
    }

protected:
    void setUp() override {
        ShardingMongodTestFixture::setUp();
        serverGlobalParams.clusterRole = ClusterRole::ShardServer;
        initializeGlobalShardingStateForMongodForTest(ConnectionString(dummyHost));

        configTargeter()->setFindHostReturnValue(dummyHost);
    }

    std::unique_ptr<DistLockCatalog> makeDistLockCatalog(ShardRegistry* shardRegistry) override {
        invariant(shardRegistry);
        return stdx::make_unique<DistLockCatalogImpl>(shardRegistry);
    }

    std::unique_ptr<DistLockManager> makeDistLockManager(
        std::unique_ptr<DistLockCatalog> distLockCatalog) override {
        return stdx::make_unique<DistLockManagerMock>(std::move(distLockCatalog));
    }

    std::unique_ptr<ShardingCatalogClient> makeShardingCatalogClient(
        std::unique_ptr<DistLockManager> distLockManager) override {
        return stdx::make_unique<ShardingCatalogClientMock>(std::move(distLockManager));
    }

    static CollectionMetadata makeEmptyMetadata() {
        const OID epoch = OID::gen();

        return CollectionMetadata(
            BSON("key" << 1),
            ChunkVersion(1, 0, epoch),
            ChunkVersion(0, 0, epoch),
            SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<CachedChunkInfo>());
    }

    /**
     * Returns a new metadata's instance based on the current state by adding a chunk with the
     * specified bounds and version. The chunk's version must be higher than that of all chunks
     * which are in the input metadata.
     *
     * It will fassert if the chunk bounds are incorrect or overlap an existing chunk or if the
     * chunk version is lower than the maximum one.
     */
    static CollectionMetadata cloneMetadataPlusChunk(const CollectionMetadata& metadata,
                                                     const BSONObj& minKey,
                                                     const BSONObj& maxKey,
                                                     const ChunkVersion& chunkVersion) {
        invariant(chunkVersion.isSet());
        invariant(chunkVersion > metadata.getCollVersion());
        invariant(minKey.woCompare(maxKey) < 0);
        invariant(!rangeMapOverlaps(metadata.getChunks(), minKey, maxKey));

        auto chunksMap = metadata.getChunks();
        chunksMap.insert(
            std::make_pair(minKey.getOwned(), CachedChunkInfo(maxKey.getOwned(), chunkVersion)));

        return CollectionMetadata(
            metadata.getKeyPattern(), chunkVersion, chunkVersion, std::move(chunksMap));
    }

    void addChunk(MetadataManager* manager) {
        ScopedCollectionMetadata scopedMetadata1 = manager->getActiveMetadata();

        ChunkVersion newVersion = scopedMetadata1->getCollVersion();
        newVersion.incMajor();
        CollectionMetadata cm2 = cloneMetadataPlusChunk(
            *scopedMetadata1.getMetadata(), BSON("key" << 0), BSON("key" << 20), newVersion);
        manager->refreshActiveMetadata(std::move(cm2));
    }

    void addTwoChunks(MetadataManager* manager, ChunkRange const& cr1, ChunkRange const& cr2) {
        ScopedCollectionMetadata scopedMetadata1 = manager->getActiveMetadata();

        ChunkVersion newVersion = scopedMetadata1->getCollVersion();
        newVersion.incMajor();
        CollectionMetadata cm1 = cloneMetadataPlusChunk(
            *scopedMetadata1.getMetadata(), cr1.getMin(), cr1.getMax(), newVersion);
        newVersion.incMajor();
        CollectionMetadata cm2 = cloneMetadataPlusChunk(
            cm1, cr2.getMin(), cr2.getMax(), newVersion);
        manager->refreshActiveMetadata(std::move(cm2));
    }

    static CollectionMetadata cloneMetadataMinusChunk(const CollectionMetadata& metadata,
                                                      const ChunkRange& range,
                                                      const ChunkVersion& chunkVersion) {
        invariant(chunkVersion.isSet());
        invariant(chunkVersion > metadata.getCollVersion());
        invariant(rangeMapOverlaps(metadata.getChunks(), range.getMin(), range.getMax()));

        auto chunksMap = metadata.getChunks();
        chunksMap.erase(range.getMin());

        return CollectionMetadata(
            metadata.getKeyPattern(), chunkVersion, chunkVersion, std::move(chunksMap));
    }

    void removeOneChunk(MetadataManager* manager, ChunkRange const& range) {
        ScopedCollectionMetadata scopedMetadata1 = manager->getActiveMetadata();

        ChunkVersion newVersion = scopedMetadata1->getCollVersion();
        newVersion.incMajor();
        CollectionMetadata cm2 = cloneMetadataMinusChunk(
            *scopedMetadata1.getMetadata(), range, newVersion);
        manager->refreshActiveMetadata(std::move(cm2));
    }
};

TEST_F(MetadataManagerTest, SetAndGetActiveMetadata) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    auto cm = makeEmptyMetadata();

    manager->refreshActiveMetadata(std::move(cm));
    addChunk(manager.get());
    ScopedCollectionMetadata scopedMetadata = manager->getActiveMetadata();

    ASSERT_EQ(1u, scopedMetadata.getMetadata()->getChunks().size());
};

TEST_F(MetadataManagerTest, ResetActiveMetadata) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());
    addChunk(manager.get());
    ScopedCollectionMetadata scopedMetadata2 = manager->getActiveMetadata();
    ASSERT_EQ(1u, scopedMetadata2.getMetadata()->getChunks().size());
};

// In the following tests, the ranges-to-clean is not drained by the background deleter thread
// because the collection involved has no CollectionShardingState, so the task just returns without
// doing anything.

TEST_F(MetadataManagerTest, CleanUpForMigrateIn) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    ChunkRange range2(BSON("key" << 10), BSON("key" << 20));
    ASSERT(manager->beginReceive(ChunkRange(BSON("key" << 0), BSON("key" << 10))));
    ASSERT(manager->beginReceive(ChunkRange(BSON("key" << 10), BSON("key" << 20))));
    ASSERT_EQ(manager->numberOfRangesToClean(), 2UL);
    ASSERT_EQ(manager->numberOfRangesToCleanStillInUse(), 0UL);
}

TEST_F(MetadataManagerTest, AddRangeNotificationsBlockAndYield) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    ChunkRange cr1(BSON("key" << 0), BSON("key" << 10));
    auto notification1 = manager->cleanUpRange(cr1);
    ASSERT(notification1.get() != nullptr && !bool(*notification1));
    ASSERT_EQ(manager->numberOfRangesToClean(), 1UL);
    auto optNotification2 = manager->trackOrphanedDataCleanup(cr1);
    ASSERT(optNotification2 && !bool(**optNotification2));  // not triggered yet
    ASSERT_EQ(notification1, *optNotification2);
    notification1->set(Status::OK());
    ASSERT_EQ(manager->numberOfRangesToClean(), 1UL);
    ASSERT_OK((*optNotification2)->get(operationContext()));
}

TEST_F(MetadataManagerTest, NotificationBlocksUntilDeletion) {
    ChunkRange cr1(BSON("key" << 0), BSON("key" << 10));
    ChunkRange cr2(BSON("key" << 50), BSON("key" << 60));
    ChunkRange cr3(BSON("key" << 80), BSON("key" << 90));
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    {
        auto scm = manager->getActiveMetadata();
        ASSERT_FALSE(scm);
    }
    manager->refreshActiveMetadata(makeEmptyMetadata());
    {
        auto scm = manager->getActiveMetadata();
        ASSERT_TRUE(scm);
        auto optNotif = manager->trackOrphanedDataCleanup(cr1);
        ASSERT(!optNotif);
    }
    
    addTwoChunks(manager.get(), cr1, cr2);  // [0,10), [50,60)
    // now we have one active tracker, one chunk, no references

    {
        auto notif = manager->cleanUpRange(cr1);
        ASSERT(notif && *notif && !notif->get(operationContext()).isOK());  // failed, range in use
        ASSERT_EQ(notif->get(operationContext()).code(), ErrorCodes::RangeOverlapConflict);
    }

    CollectionShardingState::CleanupNotification notifn1, notifn2;
    boost::optional<CollectionShardingState::CleanupNotification> optNotifn3;

    {
        ASSERT_EQ(manager->numberOfMetadataSnapshots(), 0UL);
        ASSERT_EQ(manager->numberOfRangesToClean(), 0UL);
        ASSERT_EQ(manager->numberOfRangesToCleanStillInUse(), 0UL);
        auto scm = manager->getActiveMetadata();
        // one tracker, two chunks active with reference

        removeOneChunk(manager.get(), cr1);
        // two trackers: one chunk active, two chunks with one ref on the snapshot

        ASSERT_EQ(manager->numberOfRangesToCleanStillInUse(), 0UL);

        ASSERT(bool(scm));
        notifn1 = manager->cleanUpRange(cr1);
        notifn2 = manager->cleanUpRange(cr1);
        // now two deletions on active chunk, pending release of old tracker.

        ASSERT_TRUE(notifn1 && notifn2 && notifn1.get() != notifn2.get());  // not the same referent
        ASSERT_FALSE(*notifn1 || *notifn2);  // and not triggered

        optNotifn3 = manager->trackOrphanedDataCleanup(cr1);
        ASSERT(optNotifn3 && optNotifn3->get() == notifn2.get());  // same referent as most recent

        ASSERT_EQ(manager->numberOfMetadataSnapshots(), 1UL);
        ASSERT_EQ(manager->numberOfRangesToCleanStillInUse(), 2UL);
        ASSERT_EQ(manager->numberOfRangesToClean(), 0UL);  // not yet...

    }  // scm destroyed, refcount of tracker goes to zero
    ASSERT_EQ(manager->numberOfMetadataSnapshots(), 0UL);
    ASSERT_EQ(manager->numberOfRangesToCleanStillInUse(), 0UL);
    ASSERT_EQ(manager->numberOfRangesToClean(), 2UL);
}

TEST_F(MetadataManagerTest, RefreshAfterSuccessfulMigrationSinglePending) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());
    const ChunkRange cr1(BSON("key" << 0), BSON("key" << 10));
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 0UL);
    auto notifn = manager->beginReceive(cr1);
    ASSERT_EQ(manager->numberOfRangesToClean(), 1UL);  // direct to queue
    addChunk(manager.get());
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 1UL);
    ASSERT_EQ(manager->numberOfMetadataSnapshots(), 0UL);
}


TEST_F(MetadataManagerTest, RefreshAfterSuccessfulMigrationMultiplePending) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    const ChunkRange cr1(BSON("key" << 0), BSON("key" << 10));
    const ChunkRange cr2(BSON("key" << 30), BSON("key" << 40));
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 0UL);

    {
        auto scm = manager->getActiveMetadata();
        ChunkVersion version = manager->getActiveMetadata()->getCollVersion();
        version.incMajor();

        manager->refreshActiveMetadata(cloneMetadataPlusChunk(
            *manager->getActiveMetadata().getMetadata(), cr1.getMin(), cr1.getMax(), version));
        ASSERT_EQ(manager->numberOfRangesToClean(), 0UL);
        ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 1UL);
        ASSERT_EQ(manager->numberOfMetadataSnapshots(), 1UL);
    }
    ASSERT_EQ(manager->numberOfMetadataSnapshots(), 0UL);

    {
        ChunkVersion version = manager->getActiveMetadata()->getCollVersion();
        version.incMajor();

        manager->refreshActiveMetadata(cloneMetadataPlusChunk(
            *manager->getActiveMetadata().getMetadata(), cr2.getMin(), cr2.getMax(), version));
        ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 2UL);
    }
}

TEST_F(MetadataManagerTest, RefreshAfterNotYetCompletedMigrationMultiplePending) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 0UL);

    ChunkVersion version = manager->getActiveMetadata()->getCollVersion();
    version.incMajor();

    manager->refreshActiveMetadata(
        cloneMetadataPlusChunk(*manager->getActiveMetadata().getMetadata(),
                               BSON("key" << 50),
                               BSON("key" << 60),
                               version));
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 1UL);
}

TEST_F(MetadataManagerTest, BeginReceiveWithOverlappingRange) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    CollectionShardingState::CleanupNotification notifn1, notifn2, notifn3;

    const ChunkRange cr1(BSON("key" << 0), BSON("key" << 10));
    const ChunkRange cr2(BSON("key" << 30), BSON("key" << 40));
    addTwoChunks(manager.get(), cr1, cr2);
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 2UL);
    const ChunkRange crOverlap(BSON("key" << 5), BSON("key" << 35));
    notifn1 = manager->beginReceive(crOverlap);  // not allowed
    ASSERT_TRUE(*notifn1);
    ASSERT_TRUE(notifn1->get(operationContext()).code() == ErrorCodes::RangeOverlapConflict);

    {
        auto scm = manager->getActiveMetadata();
        manager->refreshActiveMetadata(makeEmptyMetadata());
        // Even though the active chunks don't overlap, old metadata is still in use and block
        // clearing space for new chunks.
        notifn2 = manager->beginReceive(crOverlap);
        ASSERT_TRUE(*notifn2);
        ASSERT_TRUE(notifn2->get(operationContext()).code() == ErrorCodes::RangeOverlapConflict);
        // But regular cleanup requests go through.
        notifn3 = manager->cleanUpRange(crOverlap);
        // Not immediately...
        ASSERT_TRUE(!*notifn3);
    }
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 0UL);
    ASSERT_EQ(manager->numberOfMetadataSnapshots(), 0UL);
    // but eventually.
    ASSERT_EQ(manager->numberOfRangesToClean(), 1UL);
}

TEST_F(MetadataManagerTest, RefreshMetadataAfterDropAndRecreate) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    {
        auto metadata = manager->getActiveMetadata();
        ChunkVersion newVersion = metadata->getCollVersion();
        newVersion.incMajor();

        manager->refreshActiveMetadata(cloneMetadataPlusChunk(
            *metadata.getMetadata(), BSON("key" << 0), BSON("key" << 10), newVersion));
    }

    // Now, pretend that the collection was dropped and recreated
    auto recreateMetadata = makeEmptyMetadata();
    ChunkVersion newVersion = manager->getActiveMetadata()->getCollVersion();
    newVersion.incMajor();
    manager->refreshActiveMetadata(
        cloneMetadataPlusChunk(recreateMetadata, BSON("key" << 20), BSON("key" << 30), newVersion));
    ASSERT_EQ(manager->getActiveMetadata()->getChunks().size(), 1UL);

    const auto chunkEntry = manager->getActiveMetadata()->getChunks().begin();
    ASSERT_BSONOBJ_EQ(BSON("key" << 20), chunkEntry->first);
    ASSERT_BSONOBJ_EQ(BSON("key" << 30), chunkEntry->second.getMaxKey());
    ASSERT_EQ(newVersion, chunkEntry->second.getVersion());
}

// Tests membership functions for _rangesToClean
TEST_F(MetadataManagerTest, RangesToCleanMembership) {
    std::shared_ptr<MetadataManager> manager =
        std::make_shared<MetadataManager>(getServiceContext(), kNss, manager, executor());
    manager->refreshActiveMetadata(makeEmptyMetadata());

    ASSERT(manager->numberOfRangesToClean() == 0UL);

    ChunkRange cr1 = ChunkRange(BSON("key" << 0), BSON("key" << 10));
    auto notification = manager->cleanUpRange(cr1);

    ASSERT(manager->numberOfRangesToClean() == 1UL);
}

}  // namespace
}  // namespace mongo
