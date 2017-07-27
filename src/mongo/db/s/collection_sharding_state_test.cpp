/*    Copyright (C) 2016 MongoDB Inc.
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

#include "mongo/db/s/collection_sharding_state.h"

#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/s/type_shard_identity.h"
#include "mongo/s/shard_server_test_fixture.h"

namespace mongo {
namespace {

class CollShardingStateTest : public ShardServerTestFixture {
public:
    void setUp() override {
        ShardServerTestFixture::setUp();

        // Note: this assumes that globalInit will always be called on the same thread as the main
        // test thread.
        ShardingState::get(operationContext())
            ->setGlobalInitMethodForTest(
                [this](OperationContext*, const ConnectionString&, StringData) {
                    _initCallCount++;
                    return Status::OK();
                });
    }

    int getInitCallCount() const {
        return _initCallCount;
    }

private:
    int _initCallCount = 0;
};

TEST_F(CollShardingStateTest, GlobalInitGetsCalledAfterWriteCommits) {
    // Must hold a lock to call initializeFromShardIdentity, which is called by the op observer on
    // the shard identity document.
    Lock::GlobalWrite lock(operationContext());

    CollectionShardingState collShardingState(getServiceContext(),
                                              NamespaceString::kServerConfigurationNamespace);

    ShardIdentityType shardIdentity;
    shardIdentity.setConfigsvrConnString(
        ConnectionString(ConnectionString::SET, "a:1,b:2", "config"));
    shardIdentity.setShardName("a");
    shardIdentity.setClusterId(OID::gen());

    WriteUnitOfWork wuow(operationContext());
    collShardingState.onInsertOp(operationContext(), shardIdentity.toBSON());

    ASSERT_EQ(0, getInitCallCount());

    wuow.commit();

    ASSERT_EQ(1, getInitCallCount());
}

TEST_F(CollShardingStateTest, GlobalInitDoesntGetCalledIfWriteAborts) {
    // Must hold a lock to call initializeFromShardIdentity, which is called by the op observer on
    // the shard identity document.
    Lock::GlobalWrite lock(operationContext());

    CollectionShardingState collShardingState(getServiceContext(),
                                              NamespaceString::kServerConfigurationNamespace);

    ShardIdentityType shardIdentity;
    shardIdentity.setConfigsvrConnString(
        ConnectionString(ConnectionString::SET, "a:1,b:2", "config"));
    shardIdentity.setShardName("a");
    shardIdentity.setClusterId(OID::gen());

    {
        WriteUnitOfWork wuow(operationContext());
        collShardingState.onInsertOp(operationContext(), shardIdentity.toBSON());

        ASSERT_EQ(0, getInitCallCount());
    }

    ASSERT_EQ(0, getInitCallCount());
}

TEST_F(CollShardingStateTest, GlobalInitDoesntGetsCalledIfNSIsNotForShardIdentity) {
    // Must hold a lock to call initializeFromShardIdentity, which is called by the op observer on
    // the shard identity document.
    Lock::GlobalWrite lock(operationContext());

    CollectionShardingState collShardingState(getServiceContext(), NamespaceString("admin.user"));

    ShardIdentityType shardIdentity;
    shardIdentity.setConfigsvrConnString(
        ConnectionString(ConnectionString::SET, "a:1,b:2", "config"));
    shardIdentity.setShardName("a");
    shardIdentity.setClusterId(OID::gen());

    WriteUnitOfWork wuow(operationContext());
    collShardingState.onInsertOp(operationContext(), shardIdentity.toBSON());

    ASSERT_EQ(0, getInitCallCount());

    wuow.commit();

    ASSERT_EQ(0, getInitCallCount());
}

TEST_F(CollShardingStateTest, OnInsertOpThrowWithIncompleteShardIdentityDocument) {
    // Must hold a lock to call CollectionShardingState::onInsertOp.
    Lock::GlobalWrite lock(operationContext());

    CollectionShardingState collShardingState(getServiceContext(),
                                              NamespaceString::kServerConfigurationNamespace);

    ShardIdentityType shardIdentity;
    shardIdentity.setShardName("a");

    ASSERT_THROWS(collShardingState.onInsertOp(operationContext(), shardIdentity.toBSON()),
                  AssertionException);
}

TEST_F(CollShardingStateTest, GlobalInitDoesntGetsCalledIfShardIdentityDocWasNotInserted) {
    // Must hold a lock to call CollectionShardingState::onInsertOp.
    Lock::GlobalWrite lock(operationContext());

    CollectionShardingState collShardingState(getServiceContext(),
                                              NamespaceString::kServerConfigurationNamespace);

    WriteUnitOfWork wuow(operationContext());
    collShardingState.onInsertOp(operationContext(), BSON("_id" << 1));

    ASSERT_EQ(0, getInitCallCount());

    wuow.commit();

    ASSERT_EQ(0, getInitCallCount());
}

namespace {

NamespaceString testNss("testDB", "TestColl");

auto makeAMetadata() -> std::unique_ptr<CollectionMetadata> {
    const OID epoch = OID::gen();
    KeyPattern keyPattern(BSON("key" << 1 << "key3" << 1));
    auto range = ChunkRange{BSON("key" << MINKEY << "key3" << MINKEY),
                            BSON("key" << MAXKEY << "key3" << MAXKEY)};
    auto chunk = ChunkType(testNss, std::move(range), ChunkVersion(1, 0, epoch), ShardId("other"));
    auto cm = ChunkManager::makeNew(testNss, keyPattern, nullptr, false, epoch, {std::move(chunk)});
    return stdx::make_unique<CollectionMetadata>(cm, ShardId("this"));
}

}  // namespace

TEST_F(CollShardingStateTest, MakeDeleteState) {
    AutoGetCollection autoColl(operationContext(), testNss, MODE_IX);
    auto* css = CollectionShardingState::get(operationContext(), testNss);

    // clang-format off
    auto deleteState1 =
        CollectionShardingState::DeleteState(operationContext(), css, BSON("_id" << "hello"));
    ASSERT_BSONOBJ_EQ(deleteState1.documentKey, BSON("_id" << "hello"));
    ASSERT_FALSE(deleteState1.isMigrating);

    css->refreshMetadata(operationContext(), makeAMetadata());
    auto doc = BSON("_id" << "hello" << "key" << 3 << "key2" << true << "key3" << "abc");

    auto deleteState2 = CollectionShardingState::DeleteState(operationContext(), css, doc);
    ASSERT_BSONOBJ_EQ(deleteState2.documentKey,
                      BSON("key" << 3 << "key3" << "abc" << "_id" << "hello"));
    ASSERT_FALSE(deleteState2.isMigrating);
}

}  // unnamed namespace
}  // namespace mongo
