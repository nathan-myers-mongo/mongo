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
#include "mongo/db/service_context.h"

namespace mongo {

/**
 * OperationContextGroup maintains a collection of operation contexts so that they may all be killed
 * on a common event (typically, an onStepdown callback).
 */
class OperationContextGroup {
public:
    using UniqueOperationContext = ServiceContext::UniqueOperationContext;
    class Context;

    OperationContextGroup() = default;
    ~OperationContextGroup() {
        invariant(isEmpty());
    }


    /**
     * Makes an OperationContext on `client` and returns a Context object to track it.  On
     * destruction of the returned Context, the OperationContext is destroyed and its corresponding
     * entry in *this is erased.  If *this has been interrupted already, the new * context will be
     * interrupted immediately.
     */
    Context makeOperationContext(Client& client);

    /**
     * Takes ownership of the OperationContext from `ctx`, and returns a Context object to track it.
     * On destruction of the Context, its entry in *this is erased and its corresponding
     * OperationContext is destroyed. If *this has been interrupted already, `ctx` will be
     * interrupted immediately.
     */
    Context adopt(UniqueOperationContext ctx);

    /**
     * Moves the OperationContext of `ctx` from its current OperationContextGroup into *this.
     * Do this to protect an OperationContext from being interrupted along with the rest of its
     * group, or to expose `ctx` to this->interrupt().  Taking from a Context already in *this is
     * equivalent to moving from `ctx`. Taking a moved-from Context yields another moved-from
     * Context.  If *this has been interrupted already, `ctx` will be interrupted immediately.
     */
    Context take(Context ctx);

    /*
     * Interrupts all the OperationContexts maintained in *this. Any contexts subsequently added to
     * the group will be interrupted immediately.
     */
    void interrupt(ErrorCodes::Error);

    /**
     * Reports whether the group has any OperationContexts.  This should be true before the
     * destructor is called.
     */
    bool isEmpty() const {
        return _contexts.empty();
    }

private:
    friend class Context;

    /**
     * Removes the OperationContext identified by ctx from *this, and destroys it.
     */
    void _erase(Context ctx);

    std::vector<UniqueOperationContext> _contexts;
    ErrorCodes::Error _interrupted{};
};

/**
 * Context tracks one OperationContext*, and on destruction unregisters and destroys the associated
 * OperationContext.  May be used as if it were an OperationContext*.
 *
 * The lifetime of an OperationContextGroup::Context object must not exceed that of its
 * OperationContextGroup, unless it has been moved from, taken from (see
 * OperationContextGroup::take), or released.
 */
class OperationContextGroup::Context {
public:
    Context() = delete;
    Context(Context&) = delete;
    Context& operator=(Context&) = delete;
    void operator=(Context&&) = delete;
    Context(Context&& ctx) : _opCtx(ctx._opCtx), _ctxGroup(ctx._ctxGroup) {
        ctx._opCtx = nullptr;
    }
    ~Context() {
        release();
    }

    /**
     * Returns a pointer to the tracked OperationContext, or nullptr if *this has been moved from.
     */
    OperationContext* opCtx() const {
        return _opCtx;
    }

    /**
     * These enable treating a Context as if it were an OperationContext*.
     */
    operator OperationContext*() const {
        dassert(_opCtx != nullptr);
        return _opCtx;
    }
    OperationContext* operator->() const {  // because op-> will not use the conversion
        dassert(_opCtx != nullptr);
        return _opCtx;
    }

    /**
     * Destroys and unregisters the corresponding OperationContext.  After this operation, *this is
     * an xvalue, and can only be destroyed.
     */
    void release();

private:
    friend class OperationContextGroup;

    Context(OperationContext* ctx, OperationContextGroup& group) : _opCtx(ctx), _ctxGroup(group) {
        dassert(ctx);
    }

    OperationContext* _opCtx;
    OperationContextGroup& _ctxGroup;
};

}  // namespace mongo
