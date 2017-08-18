/**    Copyright 2017 MongoDB, Inc.
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

#pragma once

#include "mongo/stdx/mutex.h"
#include "mongo/util/invariant.h"

#include <utility>

namespace mongo {

/**
 * WithLock is for use solely to declare a dummy argument to functions that must be called while
 * holding a lock, as a rigorous alternative to an unchecked naming convention and/or comments to
 * indicate the requirement.
 *
 * It may be used to modernize code from (something like) this
 *
 *     // Member _lock must be held when calling this.
 *     void _clear_inlock() {
 *         _stuff = nullptr;
 *     }
 *
 * to
 *
 *     void _clear(WithLock) {
 *         _stuff = nullptr;
 *     }
 *
 * Calling such a function looks like this
 *
 *     stdx::lock_guard<stdx::mutex> lk;
 *     _clear(lk);
 *
 * Note that the formal argument need not (and should not) be named unless it is needed to pass
 * along to another function:
 *
 *     void _clear(WithLock lock) {
 *         _really_clean_up(lock);
 *     }
 *
 */
struct WithLock {
    WithLock(stdx::lock_guard<stdx::mutex> const&) noexcept {}
    WithLock(stdx::unique_lock<stdx::mutex> const& lock) {
        invariant(lock.owns_lock());
    }
    WithLock(WithLock const&) noexcept {}
    WithLock(WithLock&&) = default;

    // No assigning WithLocks.
    void operator=(WithLock const&) = delete;
    void operator=(WithLock&&) = delete;

    // No moving in a unique_lock<>.
    WithLock(stdx::unique_lock<stdx::mutex>&&) = delete;
};

}  // namespace mongo

namespace std {
// No explicitly moving a WithLock:
template <>
mongo::WithLock&& move<mongo::WithLock>(mongo::WithLock&&) = delete;
}  // namespace std
