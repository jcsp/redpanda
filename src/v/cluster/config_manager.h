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

#include "cluster/commands.h"
#include "model/record.h"

#include <seastar/core/future.hh>

namespace cluster {

/// This state machine receives updates to the global cluster config,
/// and uses them to update the per-shard configuration objects
/// that are consumed by other services within redpanda.
class config_manager final {
    static constexpr auto accepted_commands
      = make_commands_list<cluster_config_delta_cmd>{};

public:
    config_manager() {}

    ss::future<> start();
    ss::future<> stop();

    // mux_state_machine interface
    bool is_batch_applicable(const model::record_batch& b);
    ss::future<std::error_code> apply_update(model::record_batch);
};

} // namespace cluster
