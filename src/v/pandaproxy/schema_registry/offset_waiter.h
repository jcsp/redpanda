/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/std-coroutine.hh>

#include <pandaproxy/schema_registry/types.h>

namespace pandaproxy::schema_registry {

/// \brief track advancement of a topic offset, and enable waiting
///        for particular offets to be reached.
class offset_waiter {
    struct data {
        model::offset _offset;
        ss::promise<> _promise;
    };

public:
    explicit offset_waiter(size_t size = 1)
      : _waiters{size} {}

    ss::future<> wait(model::offset offset) {
        if (_offset >= offset) {
            // Already ready!
            co_return;
        }
        data d{._offset{offset}};
        auto fut = d._promise.get_future();
        co_await _waiters.push_eventually(std::move(d));
        co_return co_await std::move(fut);
    }

    /// Callers must only call this with monotonically
    /// increasing offsets increasing by 1 each time,
    /// apart from their first call which may specify any offset.
    void signal(model::offset offset) {
        assert(
          offset == _offset + model::offset{1} || _offset == model::offset{-1});
        _offset = offset;

        while (!_waiters.empty()) {
            if (_waiters.front()._offset <= offset) {
                _waiters.front()._promise.set_value();
                _waiters.pop();
            } else {
                // offsets should be ordered
                return;
            }
        }
    }

    model::offset get_offset() { return _offset; }

private:
    ss::queue<data> _waiters;
    model::offset _offset{-1};
};

} // namespace pandaproxy::schema_registry
