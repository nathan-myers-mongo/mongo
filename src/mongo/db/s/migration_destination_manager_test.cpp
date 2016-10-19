/**
 *    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/s/migration_destination_manager.h"

#include "mongo/base/status_with.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"
#include "mongo/unittest/bson_test_util.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/bson/util/bson_extract.h"

namespace mongo {

using unittest::assertGet;

namespace {

TEST(MigrationDestinationManager, SetGetState) {
    MigrationDestinationManager m;
    using State = MigrationDestinationManager::State;
    static auto const READY = MigrationDestinationManager::READY;
    static auto const ABORT = MigrationDestinationManager::ABORT;
    for (State s1 = READY; s1 <= ABORT; s1 = State(s1 + 1)) {
        m.setState(s1);
        State s2 = m.getState();
        ASSERT_EQ(s1, s2);
    }
}

TEST(MigrationDestinationManager, IsActiveInactive) {
    MigrationDestinationManager m;
    bool is = m.isActive();
    ASSERT(!is);
}

TEST(MigrationDestinationManager, ReportEmpty) {
    MigrationDestinationManager m;
    BSONObjBuilder builder;
    m.report(builder);
    auto result = builder.obj();
    ASSERT_EQ(result.nFields(), 8);

#if 0
    bool b;
    std::string s;
    BSONElement e;

    ASSERT_OK(bsonExtractBooleanField(result, "active", &b));
    ASSERT(!b);

    ASSERT_OK(bsonExtractStringField(result, "ns", &s));
    ASSERT_EQ(s, "");

    ASSERT_OK(bsonExtractStringField(result, "from", &s));
    ASSERT_EQ(s, "");

    ASSERT_OK(bsonExtractTypedField(result, "min", Object, &e));
    ASSERT(e.Obj().isEmpty());
    ASSERT_OK(bsonExtractTypedField(result, "max", Object, &e));
    ASSERT(e.Obj().isEmpty());
    ASSERT_OK(bsonExtractTypedField(result, "shardKeyPattern", Object, &e));
    ASSERT(e.Obj().isEmpty());

    ASSERT_OK(bsonExtractStringField(result, "state", &s));
    ASSERT_EQ(s, "READY");

    ASSERT(!result.hasField("errmsg"));

    ASSERT_OK(bsonExtractTypedField(result, "counts", Object, &e));
    ASSERT_BSONOBJ_EQ(e.Obj(),
        BSON("cloned" << 0 << "clonedBytes" << 0 << "catchup" << 0 << "steady" << 0));
#endif
}

TEST(MigrationDestinationManager, GetMigrationStatusReportEmpty) {
    MigrationDestinationManager m;
    BSONObj o = m.getMigrationStatusReport();
    ASSERT(o.isEmpty());
}

TEST(MigrationDestinationManager, AbortNoop) {
    const MigrationSessionId sessionId(MigrationSessionId::generate("here", "there"));
    MigrationDestinationManager m;
    bool did = m.abort(sessionId);
    ASSERT(!did);
    ASSERT(!m.isActive());
    ASSERT_EQ(m.getState(), MigrationDestinationManager::READY);
}

TEST(MigrationDestinationManager, AbortWithoutSessionIdCheckNoop) {
    MigrationDestinationManager m;
    m.abortWithoutSessionIdCheck();
    ASSERT(!m.isActive());
    ASSERT_EQ(m.getState(), MigrationDestinationManager::ABORT);
}

TEST(MigrationDestinationManager, StartCommitNot) {
    const MigrationSessionId sessionId(MigrationSessionId::generate("here", "there"));
    MigrationDestinationManager m;
    bool did = m.startCommit(sessionId);
    ASSERT(!did);
}

struct SetupInit {
    ActiveMigrationsRegistry registry;
    const NamespaceString nss;
    ScopedRegisterReceiveChunk chunk;
    const MigrationSessionId sessionId;
    const ConnectionString fromConn;
    const ShardId from;
    const ShardId to;
    const BSONObj min;
    const BSONObj max;
    const BSONObj pat;
    OID epoch;
    OID const& ep;
    const WriteConcernOptions wc;

    SetupInit()
    : nss("nss"), chunk(&registry), sessionId(MigrationSessionId::generate("here", "there"))
    , fromConn(ConnectionString::forLocal()), from("from"), to("to") 
    , min(BSON("a" << 1)), max(BSON("a" << 2)), pat(BSON("key" << 3)), ep(epoch)
        { epoch.init(); }
};

TEST(MigrationDestinationManager, StartCommit) {
    SetupInit init;
    MigrationDestinationManager m;
    
    auto status = m.start(init.nss, std::move(init.chunk), init.sessionId, init.fromConn,
        init.from, init.to, init.min, init.max, init.pat, init.ep, init.wc);
    
    ASSERT(status.isOK());
    ASSERT(m.isActive());
    
    BSONObj o = m.getMigrationStatusReport();
    std::string s;
    bool b;
    
    ASSERT_OK(bsonExtractStringField(o, "source", &s));
    ASSERT_EQ(s, init.from.toString());
    
    ASSERT_OK(bsonExtractStringField(o, "destination", &s));
    ASSERT_EQ(s, init.to.toString());
    
    ASSERT_OK(bsonExtractBooleanField(o, "isDonorShard", &b));
    ASSERT(!b);
    
    ASSERT_OK(bsonExtractStringField(o, "collection", &s));
    ASSERT_EQ(s, init.nss.toString());
    
    BSONElement element;
    ASSERT_OK(bsonExtractTypedField(o, "chunk", Object, &element));
    
    o = element.Obj();
    ASSERT_BSONOBJ_EQ(o, BSON("min" << init.min << "max" << init.max));
    
    bool did = m.startCommit(init.sessionId);
    ASSERT(!did);

}

TEST(MigrationDestinationManager, StartAbort) {
    SetupInit init;
    MigrationDestinationManager m;
    
    auto s = m.start(init.nss, std::move(init.chunk), init.sessionId, init.fromConn,
        init.from, init.to, init.min, init.max, init.pat, init.ep, init.wc);
    
    ASSERT(s.isOK());
    ASSERT(m.isActive());
    bool did = m.abort(init.sessionId);
    ASSERT(!did);
    ASSERT(m.isActive());
}

TEST(MigrationDestinationManager, StartAbortWO) {
    SetupInit init;
    MigrationDestinationManager m;
    
    auto s = m.start(init.nss, std::move(init.chunk), init.sessionId, init.fromConn,
        init.from, init.to, init.min, init.max, init.pat, init.ep, init.wc);
    
    ASSERT(s.isOK());
    ASSERT(m.isActive());
    m.abortWithoutSessionIdCheck();
    ASSERT(m.isActive());
}

}  // namespace
}  // namespace mongo
