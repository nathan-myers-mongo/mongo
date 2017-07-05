/**
 *    Copyright (C) 2017 Mongodb Inc.
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

#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"

namespace mongo {

/*
 * Maintains a collection of UniqueOperationContext objects so that they may be killed on a common
 * event, such as from a stepdown callback.  On destruction, destroys all its contexts.
 */
class OpCtxGroup {

    /*
     * Keeps one pointer to OperationContext, and on destruction unregisters and destroys the
     * associated UniqueOperationContext.
     *
     * The lifetime of an OpCtxGroup::Keeper object must not exceed that of its OpCtxGroup.
     */
    class Keeper {
    public:
        Keeper() = delete;
        Keeper(Keeper&) = delete;
        Keeper& operator=(Keeper&) = delete;
        Keeper(Keeper&&) = default;
        Keeper& operator=(Keeper&&) = default;
        ~Keeper() {
            ctx ? _ctxs._remove(_ctx) : void();
        }

        OperationContext* ctx() {
            return _ctx;
        }

    private:
        Context(OperationContext* ctx, OpCtxGroup& group) : _ctx(ctx), _ctxGroup(group) {}

        OperationContext* _ctx;
        OpCtxGroup& _ctxGroup;
    };

public:
    /*
     * Takes ownership of a UniqueOperationContext, and returns an OpCtxGroup::Context object to
     * track it.  On destruction of the Context, its entry in *this is erased.
     */
    Context adopt(UniqueOperationContext&& ctx) {
        _ctxs.emplace_back(ctx);
        return Context(_ctxs.back().get(), *this);
    }

    /*
     * Marks all the opCtx pointers maintained in *this as killed.
     */
    void kill() {
        for (auto&& uoc : _ctxs) {
            uoc.get().markAsKilled();
        }
    }

private:
    void _remove(OperationContext* ctx) {
        auto it = std::find_if(
            _ctxs.begin(), _ctxs.end(), [ctx](auto const& uoc) { return uoc.get() == ctx; });
        invariant(it != _ctxs.end());
        _ctxs.erase(it);
    }
    std::vector<UniqueOperationContext> _ctxs;
};

}  // namespace mongo
