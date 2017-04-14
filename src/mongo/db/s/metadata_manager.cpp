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
#include "mongo/bson/util/builder.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/range_arithmetic.h"
#include "mongo/db/s/collection_range_deleter.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {

using TaskExecutor = executor::TaskExecutor;
using CallbackArgs = TaskExecutor::CallbackArgs;

MetadataManager::MetadataManager(ServiceContext* sc, NamespaceString nss, TaskExecutor* executor)
    : _nss(std::move(nss)),
      _serviceContext(sc),
      _receivingChunks(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<CachedChunkInfo>()),
      _notification(std::make_shared<Notification<Status>>()),
      _executor(executor),
      _rangesToClean() {}

MetadataManager::~MetadataManager() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    if (!*_notification) {  // must check because test driver triggers it
        _notification->set(Status{ErrorCodes::InterruptedDueToReplStateChange,
                                  "tracking orphaned range deletion abandoned because the"
                                  " collection was dropped or became unsharded"});
    }
}

ScopedCollectionMetadata MetadataManager::getActiveMetadata() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    if (_activeMetadata != Ref()) {
        ++_activeMetadata->tracker.refCount;
    }
    return ScopedCollectionMetadata(_activeMetadata == Ref() ? nullptr : this, _activeMetadata);
}

size_t MetadataManager::numberOfMetadataSnapshots() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    return _metadataInUse.size() - 1;
}

void MetadataManager::unshard() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    if (_activeMetadata == Ref()) {
        invariant(_receivingChunks.empty());
        invariant(_rangesToClean.isEmpty());
        return;
    }
    log() << "Marking collection " << _nss.ns() << " with " << _activeMetadata->toStringBasic()
          << " as no longer sharded";
    _receivingChunks.clear();
    _rangesToClean.clear(Status{ErrorCodes::InterruptedDueToReplStateChange,
                                "Collection sharding metadata destroyed"});
    _metadataInUse.clear();
    _activeMetadata = Ref();
}

void MetadataManager::refreshActiveMetadata(CollectionMetadata remoteMetadata) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    // We should never be setting unsharded metadata
    invariant(!remoteMetadata.getCollVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));
    invariant(!remoteMetadata.getShardVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));

    // Collection is becoming sharded
    if (_activeMetadata == Ref()) {
        log() << "Marking collection " << _nss.ns() << " as sharded with "
              << remoteMetadata.toStringBasic();

        invariant(_receivingChunks.empty());
        invariant(_rangesToClean.isEmpty());

        _setActiveMetadata_inlock(std::move(remoteMetadata));
        return;
    }

    // If the metadata being installed has a different epoch from ours, this means the collection
    // was dropped and recreated, so we must entirely reset the metadata state
    if (_activeMetadata->getCollVersion().epoch() != remoteMetadata->getCollVersion().epoch()) {
        log() << "Overwriting metadata for collection " << _nss.ns() << " from "
              << _activeMetadataTracker->toStringBasic() << " to "
              << remoteMetadata->toStringBasic() << " due to epoch change";

        _receivingChunks.clear();
        _rangesToClean.clear(Status::OK());
        return;
    }

    // We already have a newer version
    if (_activeMetadata->getCollVersion() >= remoteMetadata.getCollVersion()) {
        LOG(1) << "Ignoring refresh of active metadata " << _activeMetadata->toStringBasic()
               << " with an older " << remoteMetadata.toStringBasic();
        return;
    }

    log() << "Refreshing metadata for collection " << _nss.ns() << " from "
          << _activeMetadata->toStringBasic() << " to " << remoteMetadata.toStringBasic();

    // Resolve any receiving chunks, which might have completed by now.
    // Should be no more than one.
    for (auto it = _receivingChunks.begin(); it != _receivingChunks.end();) {
        BSONObj const& min = it->first;
        BSONObj const& max = it->second.getMaxKey();

        if (!remoteMetadata.rangeOverlapsChunk(ChunkRange(min, max))) {
            ++it;
            continue;
        }
        // The remote metadata contains a chunk we were earlier in the process of receiving, so
        // we deem it successfully received.
        LOG(2) << "Verified chunk " << ChunkRange(min, max) << " for collection " << _nss.ns()
               << " has been migrated to this shard earlier";

        _receivingChunks.erase(it);
        it = _receivingChunks.begin();
    }

    _setActiveMetadata_inlock(std::move(remoteMetadata));
}

void MetadataManager::_setActiveMetadata_inlock(CollectionMetadata newMetadata) {
    if (_metadataInUse.empty()) {
        _metadataInUse.emplace_back(std::move(newMetadata));  // newly sharded
        _activeMetadata = _metadataInUse.begin();
    } else {
        _metadataInUse.emplace_back(std::move(newMetadata));
        ++_activeMetadata;  // point there
    }
}

// call locked
void MetadataManager::_retireExpiredMetadata() {
    bool notify = false;
    while (_activeMetadata != _metadataInUse.begin() &&
            !_metadataInUse.front().tracker.isReferenced()) {
        auto& oldMetadata = _metadataInUse.front();
        if (oldMetadata.tracker.orphans) {
            notify = true;
            log() << "Queries possibly dependent on " << _nss.ns() << " range "
                  << *oldMetadata.tracker.orphans << " finished; scheduling range for deletion";
            _pushRangeToClean(*oldMetadata.tracker.orphans);
            oldMetadata.tracker.orphans = boost::none;
        }
        _metadataInUse.pop_front();  // Discard dead metadata.
    }
    if (_activeMetadata == _metadataInUse.begin() && _activeMetadata->tracker.orphans) {
        notify = true;
        log() << "Queries possibly dependent on " << _nss.ns() << " range "
              << *_activeMetadata->tracker.orphans << " finished; scheduling range for deletion";
        _pushRangeToClean(*_activeMetadata->tracker.orphans);
        _activeMetadata->tracker.orphans = boost::none;
    }
    if (notify) {
        _notifyInUse();  // wake up waitForClean because we changed inUse
    }
}

// this is called only from ScopedCollectionMetadata members, unlocked.
void MetadataManager::_decrementUsage(ScopedCollectionMetadata const& scoped) {
    if (scoped._metadata != Ref()) {
        stdx::lock_guard<stdx::mutex> lock(_managerLock);

        dassert(scoped._metadata->tracker.isReferenced());
        if (--scoped._metadata->tracker.refCount == 0) {
            // We don't care which usageCounter went to zero.  We just expire all that are older
            // than the oldest still in use by queries. (Some start out at zero, some go to zero but
            // can't be expired yet.)

            _retireExpiredMetadata();
        }
    }
}

ScopedCollectionMetadata::ScopedCollectionMetadata() = default;

// called locked
ScopedCollectionMetadata::ScopedCollectionMetadata(
        MetadataManager* manager, MetadataManager::Ref metadata)
    : _metadata(metadata), _manager(manager) {
}

// do not call locked
ScopedCollectionMetadata::~ScopedCollectionMetadata() {
    if (_manager) {
        _manager->_decrementUsage(*this);
    }
}

CollectionMetadata* ScopedCollectionMetadata::operator->() const {
    return &*_metadata;
}

CollectionMetadata* ScopedCollectionMetadata::getMetadata() const {
    return &*_metadata;
}

// ScopedCollectionMetadata members

// do not call locked
ScopedCollectionMetadata::ScopedCollectionMetadata(ScopedCollectionMetadata&& other)
    : _metadata(other._metadata), _manager(other._manager) {
    other._metadata = MetadataManager::Ref{};
    other._manager = nullptr;
}

// do not call locked
ScopedCollectionMetadata& ScopedCollectionMetadata::operator=(ScopedCollectionMetadata&& other) {
    if (this != &other) {
        if (_manager) {
            _manager->_decrementUsage(*this);
        }
        _metadata = other._metadata;
        _manager = other._manager;
        other._metadata = MetadataManager::Ref{};
        other._manager = nullptr;
    }
    return *this;
}

ScopedCollectionMetadata::operator bool() const {
    return _manager != nullptr;
}

void MetadataManager::toBSONPending(BSONArrayBuilder& bb) const {
    for (auto it = _receivingChunks.begin(); it != _receivingChunks.end(); ++it) {
        BSONArrayBuilder pendingBB(bb.subarrayStart());
        pendingBB.append(it->first);
        pendingBB.append(it->second.getMaxKey());
        pendingBB.done();
    }
}

void MetadataManager::append(BSONObjBuilder* builder) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    _rangesToClean.append(builder);

    BSONArrayBuilder pcArr(builder->subarrayStart("pendingChunks"));
    for (const auto& entry : _receivingChunks) {
        BSONObjBuilder obj;
        ChunkRange r = ChunkRange(entry.first, entry.second.getMaxKey());
        r.append(&obj);
        pcArr.append(obj.done());
    }
    pcArr.done();

    if (_metadataInUse.empty()) {
        return;
    }

    BSONArrayBuilder amrArr(builder->subarrayStart("activeMetadataRanges"));
    for (const auto& entry : _activeMetadata->getChunks()) {
        BSONObjBuilder obj;
        ChunkRange r = ChunkRange(entry.first, entry.second.getMaxKey());
        r.append(&obj);
        amrArr.append(obj.done());
    }
    amrArr.done();
}

void MetadataManager::_scheduleCleanup(executor::TaskExecutor* executor, NamespaceString nss) {
    executor->scheduleWork([executor, nss](auto&) {
        const int maxToDelete = std::max(int(internalQueryExecYieldIterations.load()), 1);
        Client::initThreadIfNotAlready("Collection Range Deleter");
        auto UniqueOpCtx = Client::getCurrent()->makeOperationContext();
        auto opCtx = UniqueOpCtx.get();
        bool again = CollectionRangeDeleter::cleanUpNextRange(opCtx, nss, maxToDelete);
        if (again) {
            _scheduleCleanup(executor, nss);
        }
    });
}

// call locked
void MetadataManager::_pushRangeToClean(ChunkRange const& range) {
    _rangesToClean.add(range);
    if (_rangesToClean.size() == 1) {
        _scheduleCleanup(_executor, _nss);
    }
}

// call locked
void MetadataManager::_addToReceiving(ChunkRange const& range) {
    _receivingChunks.insert(
        std::make_pair(range.getMin().getOwned(),
                       CachedChunkInfo(range.getMax().getOwned(), ChunkVersion::IGNORED())));
}

bool MetadataManager::beginReceive(ChunkRange const& range) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);

    invariant(_activeMetadata != Ref{});
    if (_overlapsInUseChunk(range) || _activeMetadata->rangeOverlapsChunk(range)) {
        log() << "Rejecting in-migration to " << _nss.ns() << " range " << range
              << " because a running query might depend on documents in the range";
        return false;
    }
    _addToReceiving(range);
    _pushRangeToClean(range);
    log() << "Scheduling deletion of any documents in " << _nss.ns() << " range " << range
          << " before migrating in a chunk covering the range";
    return true;
}

// call locked
void MetadataManager::_removeFromReceiving(ChunkRange const& range) {
    auto it = _receivingChunks.find(range.getMin());
    invariant(it != _receivingChunks.end());
    _receivingChunks.erase(it);
}

void MetadataManager::forgetReceive(ChunkRange const& range) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    // This is potentially a partially received chunk, which needs to be cleaned up. We know none
    // of these documents are in use, so they can go straight to the deletion queue.
    log() << "Abandoning in-migration of " << _nss.ns() << " range " << range
          << "; scheduling deletion of any documents already copied";
    invariant(!_overlapsInUseChunk(range) &&
              !_activeMetadata->rangeOverlapsChunk(range));
    _removeFromReceiving(range);
    _pushRangeToClean(range);
}

Status MetadataManager::cleanUpRange(ChunkRange const& range) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    invariant(_activeMetadata != Ref{});

    if (_activeMetadata->rangeOverlapsChunk(range)) {
        return {ErrorCodes::RangeOverlapConflict,
                str::stream() << "Requested deletion range overlaps a live shard chunk"};
    }
    if (rangeMapOverlaps(_receivingChunks, range.getMin(), range.getMax())) {
        return {ErrorCodes::RangeOverlapConflict,
                str::stream() << "Requested deletion range overlaps a chunk being migrated in"};
    }
    if (!_overlapsInUseChunk(range)) {
        // No running queries can depend on it, so queue it for deletion immediately.
        log() << "Scheduling " << _nss.ns() << " range " << range << " for deletion";
        _pushRangeToClean(range);
    } else {
        invariant(!_metadataInUse.empty());
        if (_activeMetadata->tracker.orphans) {
            log() << "cloning active shard metadata to track another range to delete";
            _setActiveMetadata_inlock(*_activeMetadata);
        }
        _activeMetadata->tracker.orphans.emplace(
                ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()));
        log() << "Scheduling " << _nss.ns() << " range " << range
              << " for deletion after all possibly-dependent queries finish";
    }
    return Status::OK();
}

size_t MetadataManager::numberOfRangesToCleanStillInUse() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    size_t count = std::count_if(_metadataInUse.begin(), _metadataInUse.end(), [](auto& metadata) {
        return bool(metadata.tracker.orphans);
    });
    return count;
}

size_t MetadataManager::numberOfRangesToClean() {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    return _rangesToClean.size();
}

MetadataManager::CleanupNotification MetadataManager::trackOrphanedDataCleanup(
    ChunkRange const& range) {

    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    if (_overlapsInUseCleanups(range))
        return _notification;
    return _rangesToClean.overlaps(range);
}

// call locked
bool MetadataManager::_overlapsInUseChunk(ChunkRange const& range) {
    if (_activeMetadata->rangeOverlapsChunk(range)) {
        return true;  // refcount doesn't matter for the active case
    }
    for (auto it = _metadataInUse.begin(), end = _activeMetadata; it != end; ++it) {
        if (it->tracker.isReferenced() && it->rangeOverlapsChunk(range)) {
            return true;
        }
    }
    return false;
}

// call locked
bool MetadataManager::_overlapsInUseCleanups(ChunkRange const& range) {
    if (_activeMetadata->tracker.orphans && _activeMetadata->tracker.orphans->overlapWith(range)) {
        return true;
    }
    for (auto it = _metadataInUse.begin(), end = _activeMetadata; it != end; ++it) {
        if (it->tracker.orphans && bool(it->tracker.orphans->overlapWith(range))) {
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

boost::optional<KeyRange> MetadataManager::getNextOrphanRange(BSONObj const& from) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    invariant(_activeMetadata != Ref{});
    return _activeMetadata->getNextOrphanRange(_receivingChunks, from);
}

}  // namespace mongo
