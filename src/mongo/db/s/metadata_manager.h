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

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/executor/task_executor.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_range_deleter.h"
#include "mongo/db/service_context.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/util/concurrency/notification.h"

#include "mongo/stdx/memory.h"

#include <list>

namespace mongo {

class ScopedCollectionMetadata;

class MetadataManager {
    MONGO_DISALLOW_COPYING(MetadataManager);

public:
    MetadataManager(ServiceContext*, NamespaceString nss, executor::TaskExecutor* rangeDeleter);
    ~MetadataManager();

    /**
     * An ActiveMetadata must be set before this function can be called.
     *
     * Increments the usage counter of the active metadata and returns an RAII object, which
     * contains the currently active metadata.  When the usageCounter goes to zero, the RAII
     * object going out of scope will call _removeMetadata.
     */
    ScopedCollectionMetadata getActiveMetadata();

    /**
     * Returns the number of CollectionMetadata objects being maintained on behalf of running
     * queries.
     */
    size_t numberOfMetadataSnapshots() const;

    /**
     * Uses the contents of the specified metadata as a way to purge any pending chunks.
     */
    void refreshActiveMetadata(std::unique_ptr<CollectionMetadata> newMetadata);

    /**
     * Schedules any documents in the range for deletion. Assumes no active queries can see local
     * documents in the range.
     */
    void beginReceive(const ChunkRange& range);

    /**
     * Schedules any documents in the range for deletion.  Assumes no active queries can see local
     * documents in the range.
     */
    void forgetReceive(const ChunkRange& range);

    /**
     * Appends information on all the chunk ranges in rangesToClean to builder.
     */
    void append(BSONObjBuilder* builder);

    /**
     * Requests deletion of a range that was migrated away.  If the range could still be in use by
     * a running query, the cleanup is queued to be deleted only after all running queries have
     * completed.
     */
    void addRangeToClean(ChunkRange const& range);

    /**
     * Returns the number of ranges scheduled to be cleaned, exclusive of such ranges that might
     * still be in use by running queries.
     */
    size_t numberOfRangesToClean();

    using CleanupNotification = CollectionRangeDeleter::DeleteNotification;
    /**
     * Reports whether the argument range is still scheduled for deletion. If not, returns nullptr.
     * Otherwise, returns a notification n such that n->get(opCtx) will wake when deletion of a range
     * (possibly the one of interest) is completed.
     */
    CleanupNotification trackCleanup(ChunkRange const& orphans);

    /**
     * Atomically decrements the referenced count, under MetadataManager's mutex, and at zero
     * retires any metadata that has fallen out of use, pushing any orphan ranges found there to
     * the list of ranges actively being cleaned up.
     */
    void decrementTrackerUsage(uint32_t* count);


    struct CollectionMetadataTracker {
        /**
         * Creates a new CollectionMetadataTracker with the usageCounter initialized to zero.
         */
        CollectionMetadataTracker(std::unique_ptr<CollectionMetadata> m);
    private:
        friend class ScopedCollectionMetadata;
        friend class MetadataManager;
        std::unique_ptr<CollectionMetadata> metadata;
        uint32_t usageCounter{0};
        boost::optional<ChunkRange> orphans{boost::none};
    };

private:
    /**
     * Pushes current set of chunks, if any, to _metadataInUse, replaces it with newMetadata.
     */
    void _setActiveMetadata_inlock(std::unique_ptr<CollectionMetadata> newMetadata);

    /**
     * Pushes current set of chunks to _metadataInUse, replaces it with newMetadata, tagging the
     * pushed chunks with the orphan range to delete when the current set falls out of use.
     */
    void _setActiveMetadata_inlock(std::unique_ptr<CollectionMetadata> newMetadata,
                                   ChunkRange const& range);

    /**
     * Reports whether any range (possibly) still in use, but scheduled for cleanup, overlaps (any
     * part of) the argument range.
     *
     * Must be called locked.
     */
    bool _overlapsInUseCleanups(ChunkRange const& range);

    /**
     * Cleans up metadata instances no longer needed for any queries, and schedule for deletion
     * any orphaned ranges they once were suspected of depending upon.
     *
     * Must be called locked.
     */
    void _expireMetadata();

    /**
     * Adds the range to the list of ranges scheduled for immediate deletion, and schedules a
     * a background task to perform the work.
     *
     * Must be called locked.
     */
    void _pushRangeToClean(ChunkRange const& range);

    /**
     * Deletes ranges, in background, until done, using a task executor attached to the
     * ServiceContext's ShardingState, and an OperationContext generated for the occasion.
     *
     * Each time it completes a range, it wakes up clients waiting on completion of that range,
     * which may then verify their range has no more deletions scheduled, and proceed.
     */
    static void _scheduleCleanup(executor::TaskExecutor*,
                                 CollectionRangeDeleter* deleter,
                                 NamespaceString nss,
                                 stdx::mutex* metadataManagerLock);

    /**
     * Wakes up any clients waiting on a range leaving _metadataInUse
     *
     * Must be called locked.
     */
    void _notifyInUse();

    // data members

    const NamespaceString _nss;

    // ServiceContext from which to obtain instances of global support objects.
    ServiceContext* _serviceContext;

    // Mutex to protect the state below
    stdx::mutex _managerLock;

    // The currently active collection metadata
    std::unique_ptr<CollectionMetadataTracker> _activeMetadataTracker;

    // Previously active collection metadata instances still in use by active server operations or
    // cursors
    std::list<std::unique_ptr<CollectionMetadataTracker>> _metadataInUse;

    // Clients can sleep on copies of _notification while waiting for their orphan ranges to fall
    // out of use.
    std::shared_ptr<Notification<Status>> _notification;

    // The background task that deletes documents from orphaned chunk ranges.
    executor::TaskExecutor* _executor;

    // Ranges being deleted, or scheduled to be deleted, by a background task
    CollectionRangeDeleter _rangesToClean;
};

class ScopedCollectionMetadata {
    MONGO_DISALLOW_COPYING(ScopedCollectionMetadata);

public:
    /**
     * Creates an empty ScopedCollectionMetadata. Using the default constructor means that no
     * metadata is available.
     */
    ScopedCollectionMetadata();

    ~ScopedCollectionMetadata();

    ScopedCollectionMetadata(ScopedCollectionMetadata&& other);
    ScopedCollectionMetadata& operator=(ScopedCollectionMetadata&& other);

    /**
     * Dereferencing the ScopedCollectionMetadata will dereference the internal CollectionMetadata.
     */
    CollectionMetadata* operator->() const;
    CollectionMetadata* getMetadata() const;

    /**
     * True if the ScopedCollectionMetadata stores a metadata (is not empty)
     */
    operator bool() const;

private:
    friend ScopedCollectionMetadata MetadataManager::getActiveMetadata();

    /**
     * Increments the counter in the CollectionMetadataTracker.
     */
    ScopedCollectionMetadata(MetadataManager* manager,
                             MetadataManager::CollectionMetadataTracker* tracker);

    /**
     * Atomically decrements the usageCounter, and calls MetadataManager::_expireMetadata to delete
     * any newly stale CollectionMetadata records if the count has gone to zero.
     */
    void _decrementUsageCounter();

    MetadataManager* _manager{nullptr};
    MetadataManager::CollectionMetadataTracker* _tracker{nullptr};
};

}  // namespace mongo
