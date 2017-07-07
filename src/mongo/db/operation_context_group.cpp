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

#include "mongo/platform/basic.h"

#include "mongo/db/operation_context_group.h"

#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"

namespace mongo {

namespace {

template <typename C>
auto find(C& contexts, OperationContext* cp) {
    auto it = std::find_if(
        contexts.begin(), contexts.end(), [cp](auto const& opCtx) { return opCtx.get() == cp; });
    invariant(it != contexts.end());
    return it;
}

auto interruptOne(OperationContext* opCtx, ErrorCodes::Error code) {
    std::lock_guard<Client> lk(*opCtx->getClient());
    opCtx->getServiceContext()->killOperation(opCtx, code);
}

}  // namespace

auto OperationContextGroup::makeOperationContext(Client& client) -> Context {
    auto opCtx = client.makeOperationContext();
    return adopt(std::move(opCtx));
}

auto OperationContextGroup::adopt(UniqueOperationContext opCtx) -> Context {
    _contexts.emplace_back(std::move(opCtx));
    if (_interrupted) {
        interruptOne(_contexts.back().get(), _interrupted);
    }
    return Context(_contexts.back().get(), *this);
}

auto OperationContextGroup::take(Context ctx) -> Context {
    if (ctx._opCtx == nullptr) {
        return Context(nullptr, *this);  // Already been moved from.
    }
    if (&ctx._ctxGroup == this) {
        return ctx;  // Already here.
    }
    auto it = find(ctx._ctxGroup._contexts, ctx._opCtx);
    _contexts.emplace_back(std::move(*it));
    ctx._ctxGroup._contexts.erase(it);
    ctx._opCtx = nullptr;
    return Context(_contexts.back().get(), *this);
}

void OperationContextGroup::interrupt(ErrorCodes::Error code) {
    invariant(code);
    _interrupted = code;
    for (auto&& uniqueOperationContext : _contexts) {
        interruptOne(uniqueOperationContext.get(), code);
    }
}

void OperationContextGroup::_erase(Context ctx) {
    _contexts.erase(find(_contexts, ctx._opCtx));
    ctx._opCtx = nullptr;  // Keep the OperationContext from being erased again.
}

void OperationContextGroup::Context::release() {
    if (_opCtx) {
        _ctxGroup._erase(std::move(*this));
    }
}

}  // namespace mongo
