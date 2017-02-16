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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/metadata_manager.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/range_arithmetic.h"
#include "mongo/db/s/collection_range_deleter.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

using TaskExecutor = executor::TaskExecutor;
using CallbackArgs = TaskExecutor::CallbackArgs;

MetadataManager::MetadataManager( ServiceContext* sc, NamespaceString nss, TaskExecutor* executor)
    : _nss(std::move(nss)),
      _serviceContext(sc),
      _activeMetadataTracker(stdx::make_unique<CollectionMetadataTracker>(nullptr)),
      _notification(std::make_shared<Notification<Status>>()),
      _executor(executor),
      _rangesToClean() {}

MetadataManager::~MetadataManager() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    // wake everybody up to see us die
    _notification->set(Status::OK());
    _rangesToClean.clear();
}

ScopedCollectionMetadata MetadataManager::getActiveMetadata() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    if (!_activeMetadataTracker) {
        return ScopedCollectionMetadata();
    }
    return ScopedCollectionMetadata(this, _activeMetadataTracker.get());
}

void MetadataManager::refreshActiveMetadata(std::unique_ptr<CollectionMetadata> remoteMetadata) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    // Collection was never sharded in the first place. This check is necessary in order to avoid
    // extraneous logging in the not-a-shard case, because all call sites always try to get the
    // collection sharding information regardless of whether the node is sharded or not.
    if (!remoteMetadata && !_activeMetadataTracker->metadata) {
        invariant(_rangesToClean.isEmpty());
        return;
    }

    // Collection is becoming unsharded
    if (!remoteMetadata) {
        log() << "Marking collection " << _nss.ns() << " with "
              << _activeMetadataTracker->metadata->toStringBasic() << " as no longer sharded";

        _rangesToClean.clear();

        _setActiveMetadata_inlock(nullptr);
        return;
    }

    // We should never be setting unsharded metadata
    invariant(!remoteMetadata->getCollVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));
    invariant(!remoteMetadata->getShardVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));

    // Collection is becoming sharded
    if (!_activeMetadataTracker->metadata) {
        log() << "Marking collection " << _nss.ns() << " as sharded with "
              << remoteMetadata->toStringBasic();

        invariant(_rangesToClean.isEmpty());

        _setActiveMetadata_inlock(std::move(remoteMetadata));
        return;
    }

    // If the metadata being installed has a different epoch from ours, this means the collection
    // was dropped and recreated, so we must entirely reset the metadata state
    if (_activeMetadataTracker->metadata->getCollVersion().epoch() !=
        remoteMetadata->getCollVersion().epoch()) {
        log() << "Overwriting metadata for collection " << _nss.ns() << " from "
              << _activeMetadataTracker->metadata->toStringBasic() << " to "
              << remoteMetadata->toStringBasic() << " due to epoch change";

        _rangesToClean.clear();

        _setActiveMetadata_inlock(std::move(remoteMetadata));
        return;
    }

    // We already have newer version
    if (_activeMetadataTracker->metadata->getCollVersion() >= remoteMetadata->getCollVersion()) {
        LOG(1) << "Ignoring refresh of active metadata "
               << _activeMetadataTracker->metadata->toStringBasic() << " with an older "
               << remoteMetadata->toStringBasic();
        return;
    }

    log() << "Refreshing metadata for collection " << _nss.ns() << " from "
          << _activeMetadataTracker->metadata->toStringBasic() << " to "
          << remoteMetadata->toStringBasic();

    _setActiveMetadata_inlock(std::move(remoteMetadata));
}

void MetadataManager::beginReceive(const ChunkRange& range) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    // Collection is not known to be sharded if the active metadata tracker is null
    invariant(_activeMetadataTracker);

    // Any orphans in range need to be cleared, first. None of them can be in use.
    _pushRangeToClean(range);
}

void MetadataManager::forgetReceive(const ChunkRange& range) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    // This is potentially a partially received chunk, which needs to be cleaned up. We know none
    // of these documents are in use, so they can go straight to the deletion queue.
    _pushRangeToClean(range);
}

void MetadataManager::_setActiveMetadata_inlock(std::unique_ptr<CollectionMetadata> newMetadata) {
    if (_activeMetadataTracker->usageCounter > 0) {
        _metadataInUse.push_back(std::move(_activeMetadataTracker));
    }
    _activeMetadataTracker = stdx::make_unique<CollectionMetadataTracker>(std::move(newMetadata));
}

void MetadataManager::_setActiveMetadata_inlock(std::unique_ptr<CollectionMetadata> newMetadata,
                                                ChunkRange const& orphans) {
    invariant(newMetadata);
    _activeMetadataTracker->orphans.emplace(orphans.getMin().getOwned(),
                                            orphans.getMax().getOwned());
    _metadataInUse.push_back(std::move(_activeMetadataTracker));
    _activeMetadataTracker = stdx::make_unique<CollectionMetadataTracker>(std::move(newMetadata));
}

// call locked
void MetadataManager::_expireMetadata() {
    // We don't care which tracker's refcount went to zero.  We just need to expire all those
    // with zero refcount older than the oldest tracker with a nonzero refcount -- possibly
    // including the one that just went to zero.  (Some start out at zero, and some go to zero
    // but can't be expired yet.)
    bool notify = false;
    while (!_metadataInUse.empty() && _metadataInUse.front()->usageCounter == 0) {
        auto& tr = _metadataInUse.front();
        if (tr->orphans) {
            notify = true;
            _pushRangeToClean(*tr->orphans);
        }
        _metadataInUse.pop_front();  // Dispose of the tracker and the metadata it owns.
    }
    if (notify) {
        _notifyInUse();  // wake up waitForClean because we changed inUse
    }
}

// this is called from ScopedCollectionMetadata members.
void MetadataManager::decrementTrackerUsage(uint32_t* count) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    invariant(*count > 0);
    if (--*count == 0) {
        _expireMetadata();
    }
}

MetadataManager::CollectionMetadataTracker::CollectionMetadataTracker(
    std::unique_ptr<CollectionMetadata> m)
    : metadata(std::move(m)) {}

ScopedCollectionMetadata::ScopedCollectionMetadata() = default;

// called locked
ScopedCollectionMetadata::ScopedCollectionMetadata(
    MetadataManager* manager, MetadataManager::CollectionMetadataTracker* tracker)
    : _manager(manager), _tracker(tracker) {
    _tracker->usageCounter++;
}

// do not call locked
ScopedCollectionMetadata::~ScopedCollectionMetadata() {
    if (!_tracker)
        return;
    _decrementUsageCounter();
}

CollectionMetadata* ScopedCollectionMetadata::operator->() const {
    return _tracker->metadata.get();
}

CollectionMetadata* ScopedCollectionMetadata::getMetadata() const {
    return _tracker->metadata.get();
}

// ScopedCollectionMetadata members

// do not call locked
ScopedCollectionMetadata::ScopedCollectionMetadata(ScopedCollectionMetadata&& other) {
    *this = std::move(other);
}

// do not call locked
ScopedCollectionMetadata& ScopedCollectionMetadata::operator=(ScopedCollectionMetadata&& other) {
    if (this != &other) {
        // If "this" was previously initialized, make sure we perform the same logic as in the
        // destructor to decrement _tracker->usageCounter for the CollectionMetadata "this" had a
        // reference to before replacing _tracker with other._tracker.
        if (_tracker) {
            _decrementUsageCounter();
        }

        _manager = other._manager;
        _tracker = other._tracker;
        other._manager = nullptr;
        other._tracker = nullptr;
    }
    return *this;
}

void ScopedCollectionMetadata::_decrementUsageCounter() {
    invariant(_manager);
    invariant(_tracker);
    _manager->decrementTrackerUsage(&_tracker->usageCounter);
}

ScopedCollectionMetadata::operator bool() const {
    return _tracker && _tracker->metadata.get();
}

void MetadataManager::append(BSONObjBuilder* builder) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    _rangesToClean.append(builder);

    BSONArrayBuilder amrArr(builder->subarrayStart("activeMetadataRanges"));
    for (const auto& entry : _activeMetadataTracker->metadata->getChunks()) {
        BSONObjBuilder obj;
        ChunkRange r = ChunkRange(entry.first, entry.second.getMaxKey());
        r.append(&obj);
        amrArr.append(obj.done());
    }
    amrArr.done();
}

void MetadataManager::_scheduleCleanup(executor::TaskExecutor* executor,
                                       CollectionRangeDeleter* rangeDeleter,
                                       NamespaceString nss,
                                       stdx::mutex* lock) {
    executor->scheduleWork([executor, nss, rangeDeleter, lock](auto&) {
        // Note: on entry here, the collection may have been dropped and, *lock and *rangeDeleter
        // may no longer exist.  Whether they still exist can be known only after
        // CollectionRangeDeleter::cleanupNextRange constructs an AutoGetCollection. That is also
        // the reason why argument nss is passed by value.
        const int maxToDelete = std::max(int(internalQueryExecYieldIterations.load()), 1);
        Client::initThreadIfNotAlready("Collection Range Deleter");
        auto opCtx = Client::getCurrent()->makeOperationContext();
        bool more = CollectionRangeDeleter::cleanupNextRange(
            opCtx.get(), rangeDeleter, nss, lock, maxToDelete);
        if (more) {
            _scheduleCleanup(executor, rangeDeleter, nss, lock);
        }
    });
}

// call locked
void MetadataManager::_pushRangeToClean(ChunkRange const& orphans) {
    _rangesToClean.add(orphans);
    if (_rangesToClean.size() == 1) {
        _scheduleCleanup(_executor, &_rangesToClean, _nss, &_managerLock);
    }
}

void MetadataManager::addRangeToClean(ChunkRange const& range) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);

    if (!_activeMetadataTracker ||  // not sharded
        (_activeMetadataTracker->usageCounter == 0 && _metadataInUse.empty())) {
        // No running queries depend on it, so queue it up for deletion immediately.
        _pushRangeToClean(range);
    } else {
        // A runnning query might depend on the range. Queue up a copy of current metadata, tagged
        // with the range to delete when the currently running queries finish. Note the refcount
        // might be zero, making it zombie metadata that will be disposed of and its range queued
        // for deletion, when another, non-zombie, metadata's refcount goes to zero.
        _setActiveMetadata_inlock(_activeMetadataTracker->metadata->clone(), range);
    }
}

size_t MetadataManager::numberOfRangesToClean() {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    return _rangesToClean.size();
}

MetadataManager::CleanupNotification MetadataManager::trackCleanup(ChunkRange const& range) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    if (_overlapsInUseCleanups(range))
        return _notification;
    return _rangesToClean.overlaps(range);
}

// call locked
bool MetadataManager::_overlapsInUseCleanups(ChunkRange const& range) {
    for (auto& tracker : _metadataInUse) {
        if (tracker->orphans && bool(tracker->orphans->overlapWith(range))) {
            return true;
        }
    }
    return false;
}

// call locked
void MetadataManager::_notifyInUse() {
    _notification->set(Status::OK());  // wake up waitForClean
    _notification = std::make_shared<Notification<Status>>();
}

}  // namespace mongo
