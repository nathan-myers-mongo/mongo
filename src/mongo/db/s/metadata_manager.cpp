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

// One MetadataManager object is created by CollectionShardingState's constructor, and kept
// by the set of shared_ptrs pointing to it.
//
// MetadataManager maintains:
//   a list, _metadataInUse, of MetadataManager::Tracker objects, each wrapping a
//     CollectionMetadata object,
//   an iterator _activeMetadataTracker pointing to its last (sometimes only) element,
//   a CollectionRangeDeleter _rangesToClean that queues orphan ranges to delete in a background
//     thread, and
//   a map of the ranges being migrated in, _receivingChunks, to avoid deleting them.
//
// The MetadataManager::Tracker elements in _metadataInUse are kept as long as there is a
//   ScopedCollectionMetadata object with an iterator to it.  When its use count goes to zero, it
//   is considered expired, and it is erased when it becomes the oldest tracker.
//
// A Tracker object keeps:
//   a count of the ScopedCollectionMetadata objects that have pointers to it,
//   a list of Deletions, shard key ranges [min,max) of orphaned documents that may be scheduled
//     for deletion when the count goes to zero, with notifications to be signaled when deleting the
//     range is complete, or fails,
//   a CollectionMetadata, owning a map of the chunks owned by the shard,
//
//                                        _____________________________
//                              Clients: | ScopedCollectionMetadata    |
//                                       |                             |___
//  (s): std::shared_ptr<>           +---<(s) _manager  _tracker (it)>--------------------+
//  (it): list iterator              |   |_____________________________|   |__            |
//   __________________________      |  +---<(s) _manager    _tracker (it)>-----------+   |
//  | CollectionShardingState  |     |  |   |______________________________|  |       |   |
//  |      _metadataManager (s)>--+  |  |  +---<(s) _manager     _tracker (it)>---+   |   |
//  |__________________________|  |  |  |  |   |______________________________|   |   |   |
//                                |  |  |  |                                      |   |   |
//      __________________________v__v__v__v__________________________________    |   |   |
//     | MetadataManager                                                      |   |   |   |
//     |                                _____________________________  (1 use)|   |   |   |
//     |               _metadataInUse: | Tracker              (back) |<-----------+   |   |
//     |                               |  ___________________________|_       |       |   |
//     | _activeMetadataTracker(it)--->| | Tracker                     | (0 uses)     |   |
//     |                               | |  ___________________________|_     |       |   |
//     | _rangesToClean:               | | | Tracker             (front) | (2 uses)   |   |
//     |   ________________________    | | |                             |<-----------+   |
//     |  | CollectionRangeDeleter |   | | |               _usageCounter |<---------------+
//     |  |                        |   | | | _orphans                    |    |
//     |  | _orphans               |   | | |    [[min,max),notifn,       |    |
//     |  |    [[min,max),notifn,  |   | | |          ...        ]       |    |
//     |  |     [min,max),notifn,  |   | | |                             |    |
//     |  |              . . .  ]  |   | | | _metadata:                  |    |
//     |  |________________________|   | | |   _____________________     |    |
//     |                               | | |  | CollectionMetadata  |    |    |
//     | _receivingChunks              | | |  |                     |    |    |
//     |     [ [min,max),              |_| |  | _chunkVersion       |    |    |
//     |            ... ]                | |  | _chunksMap          |    |    |
//     |                                 |_|  |  . . .              |    |    |
//     |                                   |  |_____________________|    |    |
//     |                                   |_____________________________|    |
//     |______________________________________________________________________|
//
//  A ScopedCollectionMetadata object is created and held during a query, and destroyed when the
//  query no longer needs access to the collection. Its destructor decrements the Tracker's
//  usageCounter.
//
//  When a new chunk mapping replaces _activeMetadata, if any queries still depend on the current
//  mapping, it is pushed onto the back of _metadataInUse.  _activeMetadataTracker always points
//  to _metadataInUse.back().
//
//  Trackers in _metadataInUse, and their associated CollectionMetadata, are maintained at least
//  as long as any query holds a ScopedCollectionMetadata object referring to them, or to any
//  older tracker. In the diagram above, the middle Tracker must be kept until the one below it is
//  disposed of.  (Note that _metadataInUse as shown here has its front() at the bottom, back()
//  at the top. As usual, new entries are pushed onto the back, popped off the front.)

namespace mongo {

using TaskExecutor = executor::TaskExecutor;
using CallbackArgs = TaskExecutor::CallbackArgs;

struct MetadataManager::Tracker {
    /**
     * Creates a new Tracker with the usageCounter initialized to zero.
     */
    Tracker(CollectionMetadata);

    uint32_t _usageCounter{0};
    std::list<Deletion> _orphans;
    CollectionMetadata _metadata;
};

MetadataManager::MetadataManager(ServiceContext* sc,
                                 NamespaceString nss,
                                 std::shared_ptr<MetadataManager> const& self,
                                 TaskExecutor* executor)
    : _nss(std::move(nss)),
      _serviceContext(sc),
      _self(self),
      _activeMetadataTracker(Ref()),
      _receivingChunks(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<CachedChunkInfo>()),
      _executor(executor),
      _rangesToClean() {}

MetadataManager::~MetadataManager() {
    _clear();
}

// call locked, except on destruction
void MetadataManager::_clear() {
    std::list<Tracker> inUse;
    // drain any threads that might remove _metadataInUse entries, push to deleter
    inUse = std::move(_metadataInUse);
    Status status{ErrorCodes::InterruptedDueToReplStateChange,
                  "tracking orphaned range deletion abandoned because the collection was dropped"
                  " or became unsharded"};
    for (auto& tracker : _metadataInUse) {
        for (auto& deletion : tracker._orphans) {
            if (!*deletion.notification) {  // unit test might have triggered it already...
                deletion.notification->set(status);
            }
        }
    }
    _activeMetadataTracker = Ref();
    _receivingChunks.clear();
    _rangesToClean.clear(status);
}

ScopedCollectionMetadata MetadataManager::getActiveMetadata() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    if (_activeMetadataTracker != Ref()) {
        return ScopedCollectionMetadata(_self, _activeMetadataTracker);
    }
    return ScopedCollectionMetadata();
}

size_t MetadataManager::numberOfMetadataSnapshots() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    return (_activeMetadataTracker == Ref()) ? 0 : _metadataInUse.size() - 1;
}

void MetadataManager::unshard() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);

    // Collection was never sharded in the first place. This check is necessary in order to avoid
    // extraneous logging in the not-a-shard case, because all call sites always try to get the
    // collection sharding information regardless of whether the node is sharded or not.
    if (_activeMetadataTracker == Ref()) {
        invariant(_receivingChunks.empty());
        invariant(_rangesToClean.isEmpty());
        return;
    }

    // Collection is becoming unsharded
    log() << "Marking collection " << _nss.ns() << " with "
          << _activeMetadataTracker->_metadata.toStringBasic() << " as no longer sharded";
    _clear();
}

void MetadataManager::refreshActiveMetadata(CollectionMetadata remoteMetadata) {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    // We should never be setting unsharded metadata
    invariant(!remoteMetadata.getCollVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));
    invariant(!remoteMetadata.getShardVersion().isWriteCompatibleWith(ChunkVersion::UNSHARDED()));

    // Collection is becoming sharded
    if (_activeMetadataTracker == Ref()) {
        log() << "Marking collection " << _nss.ns() << " as sharded with "
              << remoteMetadata.toStringBasic();

        invariant(_receivingChunks.empty());
        invariant(_rangesToClean.isEmpty());

        _setActiveMetadata_inlock(std::move(remoteMetadata));
        return;
    }

    // If the metadata being installed has a different epoch from ours, this means the collection
    // was dropped and recreated, so we must entirely reset the metadata state
    auto version = _activeMetadataTracker->_metadata.getCollVersion();
    auto newVersion = remoteMetadata.getCollVersion();
    if (version.epoch() != newVersion.epoch()) {
        log() << "Overwriting metadata for collection " << _nss.ns() << " from "
              << _activeMetadataTracker->_metadata.toStringBasic() << " to "
              << remoteMetadata.toStringBasic() << " due to epoch change";

        _clear();
        _setActiveMetadata_inlock(std::move(remoteMetadata));
        return;
    }

    // We already have newer version
    if (version >= newVersion) {
        LOG(1) << "Ignoring refresh of active metadata "
               << _activeMetadataTracker->_metadata.toStringBasic() << " with an older "
               << remoteMetadata.toStringBasic();
        return;
    }

    log() << "Refreshing metadata for collection " << _nss.ns() << " from "
          << _activeMetadataTracker->_metadata.toStringBasic() << " to "
          << remoteMetadata.toStringBasic();

    // Resolve any receiving chunks, which might have completed by now.
    // Should be no more than one.
    for (auto it = _receivingChunks.begin(); it != _receivingChunks.end();) {
        auto range = ChunkRange(it->first, it->second.getMaxKey());
        if (!remoteMetadata.rangeOverlapsChunk(range)) {
            ++it;
            continue;
        }
        // The remote metadata contains a chunk we were earlier in the process of receiving, so
        // we deem it successfully received.
        LOG(2) << "Verified chunk " << redact(range.toString()) << " for collection " << _nss.ns()
               << " has been migrated to this shard earlier";

        _receivingChunks.erase(it);
        it = _receivingChunks.begin();
    }

    _setActiveMetadata_inlock(std::move(remoteMetadata));
}

void MetadataManager::_setActiveMetadata_inlock(CollectionMetadata newMetadata) {
    _metadataInUse.emplace_back(std::move(newMetadata));
    _activeMetadataTracker = --_metadataInUse.end();
    _retireExpiredMetadata();
}

// call locked
void MetadataManager::_retireExpiredMetadata() {
    // MetadataManager doesn't care which _usageCounter went to zero, if any.  It justs retires all
    // that are older than the oldest tracker still in use by queries. (Some start out at zero, some
    // go to zero but can't be expired yet.)  Note that new instances of ScopedCollectionMetadata
    // may get attached to the active tracker, so its usage count can increase from zero, unlike
    // most reference counts.
    dassert(!_metadataInUse.empty());
    bool notify = false;
    while (_metadataInUse.front()._usageCounter == 0) {
        auto& tracker = _metadataInUse.front();
        if (!tracker._orphans.empty()) {
            notify = true;
            log() << "Queries possibly dependent on " << _nss.ns() << " range(s) finished;"
                                                                      " scheduling for deletion";
            _pushListToClean(std::move(tracker._orphans));
        }
        if (_activeMetadataTracker == _metadataInUse.begin()) {  // no snapshots left
            break;
        };
        _metadataInUse.pop_front();  // Disconnect from the tracker (and maybe destroy it)
        dassert(!_metadataInUse.empty());
    }
}

MetadataManager::Tracker::Tracker(CollectionMetadata md) : _metadata(std::move(md)) {}

// ScopedCollectionMetadata members

// call with MetadataManager locked
ScopedCollectionMetadata::ScopedCollectionMetadata(std::shared_ptr<MetadataManager> manager,
                                                   MetadataManager::Ref tracker)
    : _tracker(tracker), _manager(std::move(manager)) {
    ++_tracker->_usageCounter;
}

ScopedCollectionMetadata::~ScopedCollectionMetadata() {
    _clear();
}

CollectionMetadata* ScopedCollectionMetadata::operator->() const {
    return _tracker != MetadataManager::Ref() ? &_tracker->_metadata : nullptr;
}

CollectionMetadata* ScopedCollectionMetadata::getMetadata() const {
    return _tracker != MetadataManager::Ref() ? &_tracker->_metadata : nullptr;
}

void ScopedCollectionMetadata::_clear() {
    if (_tracker == MetadataManager::Ref()) {
        return;
    }
    if (_manager) {
        stdx::lock_guard<stdx::mutex> managerLock(_manager->_managerLock);
        dassert(_tracker->_usageCounter != 0);
        if (--_tracker->_usageCounter == 0) {
            _manager->_retireExpiredMetadata();
        }
    }
    _tracker = MetadataManager::Ref();
    _manager.reset();
}

// do not call with MetadataManager locked
ScopedCollectionMetadata::ScopedCollectionMetadata(ScopedCollectionMetadata&& other) {
    *this = std::move(other);  // Rely on this->_tracker being zero-initialized already.
}

// do not call with MetadataManager locked
ScopedCollectionMetadata& ScopedCollectionMetadata::operator=(ScopedCollectionMetadata&& other) {
    if (this != &other) {
        _clear();
        std::swap(*this, other);
    }
    return *this;
}

ScopedCollectionMetadata::operator bool() const {
    return _tracker != MetadataManager::Ref() &&
        _manager->_activeMetadataTracker != MetadataManager::Ref();
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

    if (_activeMetadataTracker == Ref()) {
        return;
    }
    BSONArrayBuilder amrArr(builder->subarrayStart("activeMetadataRanges"));
    for (const auto& entry : _activeMetadataTracker->_metadata.getChunks()) {
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
auto MetadataManager::_pushRangeToClean(ChunkRange const& range) -> CleanupNotification {
    std::list<Deletion> ranges;
    auto notifn = std::make_shared<Notification<Status>>();
    Deletion deletion{ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()), notifn};
    ranges.emplace_back(std::move(deletion));
    _pushListToClean(std::move(ranges));
    return notifn;
}

void MetadataManager::_pushListToClean(std::list<Deletion> ranges) {
    if (_rangesToClean.add(std::move(ranges))) {
        _scheduleCleanup(_executor, _nss);
    }
}

void MetadataManager::_addToReceiving(ChunkRange const& range) {
    _receivingChunks.insert(
        std::make_pair(range.getMin().getOwned(),
                       CachedChunkInfo(range.getMax().getOwned(), ChunkVersion::IGNORED())));
}

auto MetadataManager::beginReceive(ChunkRange const& range) -> CleanupNotification {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    invariant(_activeMetadataTracker != Ref());
    invariant(!_metadataInUse.empty());

    if (_overlapsInUseChunk(range)) {
        CleanupNotification notifn = std::make_shared<Notification<Status>>();
        notifn->set({ErrorCodes::RangeOverlapConflict,
                     "Documents in target range may still be in use on the destination shard."});
        return notifn;
    }
    _addToReceiving(range);
    log() << "Scheduling deletion of any documents in " << _nss.ns() << " range "
          << redact(range.toString()) << " before migrating in a chunk covering the range";
    return _pushRangeToClean(range);
}

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

    invariant(!_overlapsInUseChunk(range));

    _removeFromReceiving(range);
    (void)_pushRangeToClean(range);
}


auto MetadataManager::cleanUpRange(ChunkRange const& range) -> CleanupNotification {

    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    invariant(_activeMetadataTracker != Ref());
    invariant(!_metadataInUse.empty());
    auto notifn = std::make_shared<Notification<Status>>();

    if (_activeMetadataTracker->_metadata.rangeOverlapsChunk(range)) {
        notifn->set({ErrorCodes::RangeOverlapConflict,
                     str::stream() << "Requested deletion range overlaps a live shard chunk"});
        return notifn;
    }

    if (rangeMapOverlaps(_receivingChunks, range.getMin(), range.getMax())) {
        notifn->set(
            {ErrorCodes::RangeOverlapConflict,
             str::stream() << "Requested deletion range overlaps a chunk being migrated in"});
        return notifn;
    }

    if (!_overlapsInUseChunk(range)) {
        // No running queries can depend on it, so queue it for deletion immediately.
        log() << "Scheduling " << _nss.ns() << " range " << redact(range.toString())
              << " for immediate deletion";

        return _pushRangeToClean(range);
    }
    invariant(_metadataInUse.begin() != _activeMetadataTracker);

    Deletion deletion{ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()), notifn};
    _activeMetadataTracker->_orphans.emplace_back(std::move(deletion));

    log() << "Scheduling " << _nss.ns() << " range " << redact(range.toString())
          << " for deletion after all possibly-dependent queries finish";

    return _activeMetadataTracker->_orphans.back().notification;
}

size_t MetadataManager::numberOfRangesToCleanStillInUse() {
    stdx::lock_guard<stdx::mutex> scopedLock(_managerLock);
    size_t count = 0;
    for (auto& tracker : _metadataInUse) {
        count += tracker._orphans.size();
    };
    return count;
}

size_t MetadataManager::numberOfRangesToClean() {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    return _rangesToClean.size();
}

auto MetadataManager::trackOrphanedDataCleanup(ChunkRange const& range)
    -> boost::optional<CleanupNotification> {

    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    auto overlaps = _overlapsInUseCleanups(range);
    return overlaps ? overlaps : _rangesToClean.overlaps(range);
}

// call locked
bool MetadataManager::_overlapsInUseChunk(ChunkRange const& range) const {
    for (auto& tracker : _metadataInUse) {
        if ((tracker._usageCounter != 0 || &tracker == &_metadataInUse.back()) &&
            tracker._metadata.rangeOverlapsChunk(range)) {
            return true;
        }
    }
    return false;
}

// call locked
auto MetadataManager::_overlapsInUseCleanups(ChunkRange const& range) const
    -> boost::optional<CleanupNotification> {
    // search newest to oldest
    auto tracker = _metadataInUse.crbegin(), et = _metadataInUse.crend();
    for (; tracker != et; ++tracker) {
        auto cleanup = tracker->_orphans.crbegin(), ec = tracker->_orphans.crend();
        for (; cleanup != ec; ++cleanup) {
            if (bool(cleanup->range.overlapWith(range))) {
                return cleanup->notification;
            }
        }
    }
    return boost::none;
}

boost::optional<KeyRange> MetadataManager::getNextOrphanRange(BSONObj const& from) {
    stdx::unique_lock<stdx::mutex> scopedLock(_managerLock);
    invariant(_activeMetadataTracker != Ref());
    return _activeMetadataTracker->_metadata.getNextOrphanRange(_receivingChunks, from);
}

}  // namespace mongo
