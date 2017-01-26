/**
 * Copyright (C) 2016 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include <boost/optional.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/optime.h"
#include "mongo/db/repl/read_concern_args.h"
#include "mongo/s/shard_id.h"

namespace mongo {

class BatchedCommandRequest;
class BatchedCommandResponse;
class OperationContext;
class RemoteCommandTargeter;

/**
 * Presents an interface for talking to shards, regardless of whether that shard is remote or is
 * the current (local) shard.
 */
class Shard {
public:
    struct CommandResponse {
        CommandResponse(BSONObj _response,
                        BSONObj _metadata,
                        Status _commandStatus,
                        Status _writeConcernStatus)
            : response(std::move(_response)),
              metadata(std::move(_metadata)),
              commandStatus(std::move(_commandStatus)),
              writeConcernStatus(std::move(_writeConcernStatus)) {}

        /**
         * Takes the response from running a batch write command and writes the appropriate response
         * into batchResponse, while also returning the Status of the operation.
         */
        static Status processBatchWriteResponse(StatusWith<CommandResponse> response,
                                                BatchedCommandResponse* batchResponse);

        BSONObj response;
        BSONObj metadata;
        Status commandStatus;
        Status writeConcernStatus;
    };

    struct QueryResponse {
        std::vector<BSONObj> docs;
        repl::OpTime opTime;
    };

    enum class RetryPolicy {
        kIdempotent,
        kNotIdempotent,
        kNoRetry,
    };

    const ShardId getId() const;

    /**
     * Returns true if this shard object represents the config server.
     */
    bool isConfig() const;

    /**
     * Returns the current connection string for the shard.
     */
    ConnectionString getConnString() const;

    /**
     * Returns the connection string that was used to create the Shard from the ShardFactory.  The
     * current connection string may be different.
     * NOTE: Chances are this isn't the method you want.  When in doubt, prefer to use
     * getConnString() instead.
     */
    ConnectionString originalConnString() const;

    /**
     * Returns the RemoteCommandTargeter for the hosts in this shard.
     *
     * This is only valid to call on ShardRemote instances.
     */
    std::shared_ptr<RemoteCommandTargeter> getTargeter() const;

    /**
     * Notifies the RemoteCommandTargeter owned by the shard of a particular mode of failure for
     * the specified host.
     *
     * This is only valid to call on ShardRemote instances.
     */
    void updateReplSetMonitor(const HostAndPort& remoteHost,
                                      const Status& remoteCommandStatus) const;

    /**
     * Returns a string description of this shard entry.
     */
    std::string toString() const;

    /**
     * Returns whether a server operation which failed with the given error code should be retried
     * (i.e. is safe to retry and has the potential to succeed next time).  The 'options' argument
     * describes whether the operation that generated the given code was idempotent, which affects
     * which codes are safe to retry on.
     */
    bool isRetriableError(ErrorCodes::Error code, RetryPolicy options) const;

    /**
     * Runs the specified command returns the BSON command response plus parsed out Status of this
     * response and write concern error (if present). Retries failed operations according to the
     * given "retryPolicy".  Retries indefinitely until/unless a non-retriable error is encountered,
     * the maxTimeMs on the OperationContext expires, or the operation is interrupted.
     */
    StatusWith<CommandResponse> runCommand(OperationContext* txn,
                                           const ReadPreferenceSetting& readPref,
                                           const std::string& dbName,
                                           const BSONObj& cmdObj,
                                           RetryPolicy retryPolicy) const;

    /**
     * Same as the other variant of runCommand, but allows the operation timeout to be overriden.
     * Runs for the lesser of the remaining time on the operation context or the specified maxTimeMS
     * override.
     */
    StatusWith<CommandResponse> runCommand(OperationContext* txn,
                                           const ReadPreferenceSetting& readPref,
                                           const std::string& dbName,
                                           const BSONObj& cmdObj,
                                           Milliseconds maxTimeMSOverride,
                                           RetryPolicy retryPolicy) const;

    /**
     * Same as runCommand, but will only retry failed operations up to 3 times, regardless of
     * the retryPolicy or the remaining maxTimeMs.
     * Wherever possible this method should be avoided in favor of runCommand.
     */
    StatusWith<CommandResponse> runCommandWithFixedRetryAttempts(
        OperationContext* txn,
        const ReadPreferenceSetting& readPref,
        const std::string& dbName,
        const BSONObj& cmdObj,
        RetryPolicy retryPolicy) const;

    /**
     * Same as runCommand, but will only retry failed operations up to 3 times, regardless of
     * the retryPolicy or the remaining maxTimeMs.
     * Wherever possible this method should be avoided in favor of runCommand.
     */
    StatusWith<CommandResponse> runCommandWithFixedRetryAttempts(
        OperationContext* txn,
        const ReadPreferenceSetting& readPref,
        const std::string& dbName,
        const BSONObj& cmdObj,
        Milliseconds maxTimeMSOverride,
        RetryPolicy retryPolicy) const;

    /**
     * Expects a single-entry batch wrtie command and runs it on the config server's primary using
     * the specified retry policy.
     */
    BatchedCommandResponse runBatchWriteCommandOnConfig(OperationContext* txn,
                                                        const BatchedCommandRequest& batchRequest,
                                                        RetryPolicy retryPolicy) const;

    /**
    * Warning: This method exhausts the cursor and pulls all data into memory.
    * Do not use other than for very small (i.e., admin or metadata) collections.
    * Performs retries if the query fails in accordance with the kIdempotent RetryPolicy.
    *
    * ShardRemote instances expect "readConcernLevel" to always be kMajorityReadConcern, whereas
    * ShardLocal instances expect either kLocalReadConcern or kMajorityReadConcern.
    */
    StatusWith<QueryResponse> exhaustiveFindOnConfig(OperationContext* txn,
                                                     const ReadPreferenceSetting& readPref,
                                                     const repl::ReadConcernLevel& readConcernLevel,
                                                     const NamespaceString& nss,
                                                     const BSONObj& query,
                                                     const BSONObj& sort,
                                                     const boost::optional<long long> limit) const;

    /**
     * Builds an index on a config server collection.
     * Creates the collection if it doesn't yet exist.  Does not error if the index already exists,
     * so long as the options are the same.
     * NOTE: Currently only supported for LocalShard.
     */
    Status createIndexOnConfig(OperationContext* txn,
                                       const NamespaceString& ns,
                                       const BSONObj& keys,
                                       bool unique) const;

    // This timeout will be used by default in operations against the config server, unless
    // explicitly overridden
    static const Milliseconds kDefaultConfigCommandTimeout;

    /**
     * Returns false if the error is a retriable error and/or causes a replset monitor update. These
     * errors, if from a remote call, should not be further propagated back to another server
     * because that server will interpret them as orignating on this server rather than the one this
     * server called.
     */
    static bool shouldErrorBePropagated(ErrorCodes::Error code);

    Shard(Shard const&) = default;
    Shard(Shard&&) = default;
    Shard& operator=(Shard const&) = default;
    Shard& operator=(Shard&&) = default;

    class ImplRemote;
    class ImplLocal;
private:
    class Impl;
    explicit Shard(std::shared_ptr<Impl> impl)
        : _impl(std::move(impl)) { invariant(_impl.get() != nullptr); }
    Shard() = delete;

    friend class ShardFactory;
    std::shared_ptr<Impl> _impl;
};

class Shard::Impl {
    Impl(ShardId id) : _id(id) {}
public:
    virtual ~Impl() = default;

    friend class Shard::ImplRemote;
    friend class Shard::ImplLocal;
    friend class Shard;

    using CommandResponse = Shard::CommandResponse;
    using QueryResponse = Shard::QueryResponse;
    using RetryPolicy = Shard::RetryPolicy;

    const ShardId getId() const;
    bool isConfig() const;
    virtual ConnectionString getConnString() const = 0;
    virtual ConnectionString originalConnString() const = 0;
    virtual std::shared_ptr<RemoteCommandTargeter> getTargeter() const = 0;
    virtual void updateReplSetMonitor(const HostAndPort& remoteHost,
                                      const Status& remoteCommandStatus) = 0;
    virtual std::string toString() const = 0;
    virtual bool isRetriableError(ErrorCodes::Error code, RetryPolicy options) = 0;
    virtual Status createIndexOnConfig(OperationContext* txn,
                                        const NamespaceString& ns,
                                        const BSONObj& keys,
                                        bool unique) = 0;

    struct HostWithResponse {
        HostWithResponse(boost::optional<HostAndPort> _host,
                         StatusWith<CommandResponse> _commandResponse)
            : host(std::move(_host)), commandResponse(std::move(_commandResponse)) {}
        boost::optional<HostAndPort> host;
        StatusWith<CommandResponse> commandResponse;
    };

    /**
     * Runs the specified command against the shard backed by this object with a timeout set to the
     * minimum of maxTimeMSOverride or the timeout of the OperationContext.
     *
     * The return value exposes RemoteShard's host for calls to updateReplSetMonitor.
     *
     * NOTE: LocalShard implementation will not return a valid host and so should be ignored.
     */
    virtual HostWithResponse _runCommand(OperationContext* txn,
                                         const ReadPreferenceSetting& readPref,
                                         const std::string& dbname,
                                         Milliseconds maxTimeMSOverride,
                                         const BSONObj& cmdObj) = 0;

    virtual StatusWith<QueryResponse> _exhaustiveFindOnConfig(
        OperationContext* txn,
        const ReadPreferenceSetting& readPref,
        const repl::ReadConcernLevel& readConcernLevel,
        const NamespaceString& nss,
        const BSONObj& query,
        const BSONObj& sort,
        boost::optional<long long> limit) = 0;

    /**
     * Identifier of the shard as obtained from the configuration data (i.e. shard0000).
     */
    const ShardId _id;
};

inline std::string Shard::toString() const { return _impl->toString(); }
inline const ShardId Shard::getId() const { return _impl->getId(); }
inline bool Shard::isConfig() const { return _impl->isConfig(); }
inline ConnectionString Shard::getConnString() const { return _impl->getConnString(); }
inline ConnectionString Shard::originalConnString() const
    { return _impl->originalConnString(); }
inline std::shared_ptr<RemoteCommandTargeter> Shard::getTargeter() const
    { return _impl->getTargeter(); }
inline void Shard::updateReplSetMonitor(const HostAndPort& remoteHost,
                                 const Status& remoteCommandStatus) const
    { return _impl->updateReplSetMonitor(remoteHost, remoteCommandStatus); }
 inline bool Shard::isRetriableError(ErrorCodes::Error code, RetryPolicy options) const
    { return _impl->isRetriableError(code, options); }
 inline Status Shard::createIndexOnConfig(OperationContext* txn,
                            const NamespaceString& ns,
                            const BSONObj& keys,
                            bool unique) const
    { return _impl->createIndexOnConfig(txn, ns, keys, unique); }

}  // namespace mongo
