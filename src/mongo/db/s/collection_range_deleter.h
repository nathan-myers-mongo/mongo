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
#include "mongo/db/namespace_string.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/util/concurrency/notification.h"

namespace mongo {

class BSONObj;
class Collection;
class OperationContext;

class CollectionRangeDeleter {
    MONGO_DISALLOW_COPYING(CollectionRangeDeleter);

public:
    /**
     * Normally, construct with the collection name and ShardingState's dedicated executor.
     */
    CollectionRangeDeleter() = default;
    ~CollectionRangeDeleter();

    using DeleteNotification = std::shared_ptr<Notification<Status>>;

    /**
     * Adds a new range to be cleaned up by the cleaner thread.
     */
    void add(const ChunkRange& range);

    /**
     * Reports whether the argument range overlaps any of the ranges to clean.  If there is overlap,
     * it returns a notification that will be completed when the currently newest overlapping
     * range is no longer scheduled.  Its value indicates whether it has been successfully removed.
     * If there is no overlap, the result is nullptr.  After a successful removal, the caller
     * should call again to ensure no other range overlaps the argument.
     * (See MigrationDestinationManager::waitForClean and MetadataManager::trackCleanup for an
     * example use.)
     */
    DeleteNotification overlaps(ChunkRange const& range);

    /**
     * Reports the number of ranges remaining to be cleaned up.
     */
    size_t size() const;

    bool isEmpty() const;

    /*
     * Notify anything waiting on ranges scheduled, before discarding the ranges.
     */
    void clear();

    /*
     * Append a representation of self to the specified builder.
     */
    void append(BSONObjBuilder* builder) const;

    /**
     * Acquires the collection IX lock and, if the collection still exists and *self has ranges
     * scheduled to clean, deletes up to maxToDelete documents, notifying watchers of ranges as they
     * are completed.  Uses specified lock to serialize access to *self.
     *
     * Returns true if it should be run again, false if there is no more progress to be made.
     */
    static bool cleanupNextRange(OperationContext* opCtx,
                                 CollectionRangeDeleter* self,
                                 NamespaceString const& nss,
                                 stdx::mutex* lock,
                                 int maxToDelete);

private:
    /**
     * Removes the latest-scheduled range from the ranges to be cleaned up.
     */
    void _pop(Status);

    /**
     * Performs the deletion of up to maxToDelete entries within the range in progress.
     *
     * Returns the number of documents deleted, 0 if done with the range.
     */
    StatusWith<int> _doDeletion(OperationContext* opCtx,
                                Collection* collection,
                                ChunkRange const& range,
                                const BSONObj& keyPattern,
                                int maxToDelete);

    // Ranges scheduled for deletion.  The front of the list will be in active process of deletion.
    // As each range is completed, its notification is signaled before it is popped.

    struct Deletion {
        ChunkRange const range;
        DeleteNotification const notification;
    };
    std::list<Deletion> _orphans;
};

}  // namespace mongo
