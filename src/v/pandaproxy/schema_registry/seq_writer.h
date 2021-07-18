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

class seq_writer {
public:
    seq_writer(ss::sharded<kafka::client::client>& client, sharded_store& store)
      : _client(client)
      , _store(store){};

    ss::future<> read_sync();

    ss::future<schema_id>
    write_subject_version(subject sub, schema_definition def, schema_type type);

private:
    ss::future<std::optional<schema_id>> write_subject_version_inner(
      model::offset write_at,
      subject sub,
      schema_definition def,
      schema_type type);

    ss::future<> read_sync_inner();

    ss::sharded<kafka::client::client>& _client;
    sharded_store& _store;
};

} // namespace pandaproxy::schema_registry
