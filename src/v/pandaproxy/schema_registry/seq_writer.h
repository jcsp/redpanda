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

    ss::future<std::vector<schema_version>>
    delete_subject_impermanent(subject sub);

    ss::future<std::vector<schema_version>>
    delete_subject_permanent(subject sub);

private:
    ss::future<> read_sync_inner();

    ss::sharded<kafka::client::client>& _client;
    sharded_store& _store;

    ss::future<> back_off();

    /// Helper for write paths that use sequence+retry logic to synchronize
    /// multiple writing nodes.
    template<typename V, typename F>
    ss::future<V> sequenced_write(F f) {
        while (true) {
            auto next_offset = co_await _store.project_write();
            std::optional<V> r = co_await f(next_offset);
            co_await _store.complete_write();

            if (r.has_value()) {
                co_return r.value();
            } else {
                co_await back_off();
            }
        }
    }

    ss::future<bool>
    produce_and_check(model::offset write_at, model::record_batch batch);
};

} // namespace pandaproxy::schema_registry
