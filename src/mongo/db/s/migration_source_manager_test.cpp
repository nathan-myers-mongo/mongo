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

#include "mongo/db/s/migration_session_id.h"

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/jsobj.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

using unittest::assertGet;

namespace {

TEST(MigrationSourceManager, GetNSS) {
    MigrationSourceManager m;
    NamespaceString ns = getNss();
}

TEST(MigrationSourceManager, StartClone) {
    MigrationSourceManager m;
    OperationContext txn;
    Status s = startClone(txn);
}

TEST(MigrationSourceManager, AwaitToCatchUp) {
    MigrationSourceManager m;
    OperationContext txn;
    Status s = startClone(txn);
    m.AwaitToCatchUp(txn);
}

TEST(MigrationSourceManager, EnterCriticalSection) {
    MigrationSourceManager m;
    OperationContext txn;
}

TEST(MigrationSourceManager, CommitDonateChunk) {
    MigrationSourceManager m;
    OperationContext txn;
}

TEST(MigrationSourceManager, CleanupOnError) {
    MigrationSourceManager m;
    OperationContext txn;
}

TEST(MigrationSourceManager, GetKeyPattern) {
    MigrationSourceManager m;
}

TEST(MigrationSourceManager, GetCloner) {
    MigrationSourceManager m;
}

TEST(MigrationSourceManager, GetMigrationCriticalSectionSignal) {
    MigrationSourceManager m;
    std::shared_ptr<Notification<void>> p = m.getMigrationCriticalSectionSignal();
}

TEST(MigrationSourceManager, GetMigrationStatusReport) {
    MigrationSourceManager m;
    BSONObj o = m.getMigrationStatusReport();
}

}  // namespace
}  // namespace mongo
