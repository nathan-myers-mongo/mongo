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

#include "mongo/db/opctx_group.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"

namespace mongo {

auto OpCtxGroup::adopt(UniqueOperationContext&& ctx) -> OpCtxGroup::Context {
    _contexts.emplace_back(std::forward<UniqueOperationContext>(ctx));
    return Context(_contexts.back().get(), *this);
}

void OpCtxGroup::interrupt(ErrorCodes::Error code) {
    for (auto&& uniqueOperationContext : _contexts) {
        auto* opCtx = uniqueOperationContext.get();
        opCtx->getServiceContext()->killOperation(opCtx, code);
    }
}

void OpCtxGroup::_erase(OperationContext* ctx) {
    auto it = std::find_if(
        _contexts.begin(), _contexts.end(), [ctx](auto const& uoc) { return uoc.get() == ctx; });
    invariant(it != _contexts.end());
    _contexts.erase(it);
}

}  // namespace mongo
