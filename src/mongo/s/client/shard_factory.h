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

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "mongo/base/disallow_copying.h"

#include "mongo/client/connection_string.h"
#include "mongo/client/remote_command_targeter_factory.h"
#include "mongo/s/client/shard.h"
#include "mongo/stdx/functional.h"

namespace mongo {

/**
 * An object factory for creating Shard instances via calling registered builders.
 */
class ShardFactory {
    MONGO_DISALLOW_COPYING(ShardFactory);

public:
    ShardFactory(std::unique_ptr<RemoteCommandTargeterFactory>);
    ~ShardFactory() = default;

    /**
     * Creates a shared_ptr with a new instance of a Shard with the provided shardId
     * and connection string.
     */
    Shard createShard(const ShardId& shardId, const ConnectionString& connStr);

private:
    // Even though ShardFactory doesn't use _targeterFactory directly, the functions contained in
    // _builders may, so ShardFactory must own _targeterFactory so that their lifetimes are tied
    // together
    std::unique_ptr<RemoteCommandTargeterFactory> _targeterFactory;
};

}  // namespace mongo
