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
    std::cout << '\n' << s << '\n';
    ASSERT_EQ(s, "READY");

    ASSERT(!result.hasField("errmsg"));

    ASSERT_OK(bsonExtractTypedField(result, "counts", Object, &e));
    ASSERT_BSONOBJ_EQ(e.Obj(),
        BSON("cloned" << 0 << "clonedBytes" << 0 << "catchup" << 0 << "steady" << 0));
}

TEST(MigrationDestinationManager, GetMigrationStatusReportEmpty) {
    MigrationDestinationManager m;
    BSONObj o = m.getMigrationStatusReport();
    ASSERT(o.isEmpty());
}

struct range {
    range begin() { return *this; }
    range end() { return *this; }
    range& operator++() { ++lo_; return *this; }
    int operator*() { return lo_; }
    friend bool operator!=(range const& a, range const& b) { return a.lo_ != b.hi_; }
    int lo_, hi_;
};

TEST(MigrationDestinationManager, Start) {
    const WriteConcernOptions wc;

    for (auto i : range{0,3}) {
        ActiveMigrationsRegistry registry;
        MigrationDestinationManager m;
        const NamespaceString nss("nss");
        ScopedRegisterReceiveChunk chunk(&registry);
        const MigrationSessionId sessionId(MigrationSessionId::generate("here", "there"));
        const ConnectionString fromConn(ConnectionString::forLocal());
        const ShardId from("from");
        const ShardId to("to");
        const BSONObj min = BSON("a" << 1);
        const BSONObj max = BSON("a" << 2);
        const BSONObj pat = BSON("key" << 3);
        OID epoch; epoch.init();
        OID const& ep = epoch;
        const WriteConcernOptions wc;
        
        auto s =
            m.start(nss, std::move(chunk), sessionId, fromConn, from, to, min, max, pat, ep, wc);

        ASSERT(s.isOK());
        ASSERT(m.isActive());

        switch (i) {
        case 0: {

            BSONObj o = m.getMigrationStatusReport();
            std::string s;
            bool b;

            ASSERT_OK(bsonExtractStringField(o, "source", &s));
            ASSERT_EQ(s, from.toString());

            ASSERT_OK(bsonExtractStringField(o, "destination", &s));
            ASSERT_EQ(s, to.toString());

            ASSERT_OK(bsonExtractBooleanField(o, "isDonorShard", &b));
            ASSERT(b);

            ASSERT_OK(bsonExtractStringField(o, "collection", &s));
            ASSERT_EQ(s, nss.toString());
            
            BSONElement element;
            ASSERT_OK(bsonExtractTypedField(o, "chunk", Object, &element));

            o = element.Obj();

            ASSERT_BSONOBJ_EQ(o, BSON("min" << min << "max" << max));

            bool did = m.abort(sessionId);
            ASSERT(did);
            ASSERT(m.isActive());

        } break;
        case 1: {
            m.abortWithoutSessionIdCheck();
            ASSERT(m.isActive());
        } break;
        case 2: {
            bool did = m.startCommit(sessionId);
            ASSERT(did);
        } break; }
    }
}
#if 0

TEST(MigrationDestinationManager, AbortNoop) {
    const MigrationSessionId sessionId;
    MigrationDestinationManager m;
    bool did = m.abort(sessionId);
    ASSERT(!did);
    ASSERT(!m.isActive());
    ASSERT_EQ(m.getState(), MigrationDestinationManager::ABORT);
}

TEST(MigrationDestinationManager, AbortWithoutSessionIdCheckNoop) {
    MigrationDestinationManager m;
    m.abortWithoutSessionIdCheck();
    ASSERT(!m.isActive());
    ASSERT_EQ(m.getState(), MigrationDestinationManager::ABORT);
}

TEST(MigrationDestinationManager, StartCommitNot) {
    const MigrationSessionId sessionId;
    MigrationDestinationManager m;
    bool did = m.startCommit(sessionId);
    ASSERT(!did);
}
#endif

}  // namespace
}  // namespace mongo
