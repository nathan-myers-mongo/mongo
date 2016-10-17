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
    BSONObjBuilder b;
    m.report(b);
    auto o = b.obj();
    ASSERT_EQ(o.nFields(), 1);
    ASSERT(o.hasField("active"));
    auto e = o["active"];
    ASSERT(e.isBoolean());
    ASSERT(e.boolean());
}

TEST(MigrationDestinationManager, GetMigrationStatusReportEmpty) {
    MigrationDestinationManager m;
    BSONObj o = m.getMigrationStatusReport();
    ASSERT(o.isEmpty());
}

Status start(ActiveMigrationsRegistry& registry, MigrationDestinationManager& m)
{
    ScopedRegisterReceiveChunk chunk(&registry);
    const MigrationSessionId sessionId(MigrationSessionId::generate("here", "there"));
    const ConnectionString fromConn(ConnectionString::forLocal());
    const ShardId from("from");
    const ShardId to("to");
    const BSONObj min = BSON(" ");
    const BSONObj max = BSON(" ");
    const BSONObj pat = BSON(" ");
    const OID epoch = 100;

    return m.start(std::move(chunk), sessionId, fromConn, from, to, min, max, pat, epoch, wc);
}


TEST(MigrationDestinationManager, Start) {
    const WriteConcernOptions wc;

    {
        ActiveMigrationsRegistry registry;
        MigrationDestinationManager m;

        auto s = start(registry, m)
        ASSERT(s.isOK());
        ASSERT(m.isActive());

        BSONObj o = m.getMigrationStatusReport(b);
        ASSERT_EQ(o,
            BSON("active" << true
            << "sessionId" << 12345
            << "ns" << ns
            << "from" << from
            << "min" << min
            << "max" << max
            << "shardKeyPattern" << pat
            << "state" << "READY"
            << "counts" << BSON("cloned" << 0
                << "clonedBytes" << 0
                << "catchup" << 0
                << "steady" << 0)
            ));
    }
    { 
        ActiveMigrationsRegistry registry;
        MigrationDestinationManager m;
        Status s = start(registry, m);
        bool did = m.abort(sessionId);
        ASSERT(did);
        ASSERT(m.isActive());
    }
    { 
        ActiveMigrationsRegistry registry;
        MigrationDestinationManager m;
        Status s = start(registry, m);
        m.abortWithoutSessionIdCheck();
        ASSERT(m.isActive());
    }
    { 
        ActiveMigrationsRegistry registry;
        MigrationDestinationManager m;
        Status s = start(registry, m);
        bool did = m.startCommit(sessionId);
        ASSERT(did);
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
