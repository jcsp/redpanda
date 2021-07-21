//// Copyright 2021 Vectorized, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#pragma once

#include "kafka/client/client.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/types.h"
#include "utils/retry.h"

namespace pandaproxy::schema_registry {

using namespace std::chrono_literals;

class seq_writer {
public:
    seq_writer(ss::sharded<kafka::client::client>& client, sharded_store& store)
      : _client(client)
      , _store(store){};

    ss::future<> read_sync();

    ss::future<schema_id>
    write_subject_version(subject sub, schema_definition def, schema_type type);

    ss::future<bool>
    write_config(std::optional<subject> sub, compatibility_level compat);

    ss::future<bool>
    delete_subject_version(subject sub, schema_version version);

    ss::future<std::vector<schema_version>>
    delete_subject_impermanent(subject sub);

    ss::future<std::vector<schema_version>> delete_subject_permanent(
      subject sub, std::optional<schema_version> version);

private:
    ss::future<> read_sync_inner();

    ss::sharded<kafka::client::client>& _client;
    sharded_store& _store;

    struct WriteCollision : public std::exception {
        const char* what() const throw() { return "Write Collision"; }
    };

    /// Helper for write paths that use sequence+retry logic to synchronize
    /// multiple writing nodes.
    template<typename V, typename F>
    ss::future<V> sequenced_write(F f) {
        auto fn = [this, f]() -> ss::future<V> {
            auto next_offset = co_await _store.begin_write();
            std::optional<V> r = co_await f(next_offset);
            co_await _store.complete_write();

            if (r.has_value()) {
                co_return r.value();
            } else {
                co_await read_sync();
                throw WriteCollision();
            }
        };

        return retry_with_backoff(
          std::numeric_limits<int>::max(), std::move(fn));
    }

    ss::future<bool>
    produce_and_check(model::offset write_at, model::record_batch batch);
};

} // namespace pandaproxy::schema_registry
