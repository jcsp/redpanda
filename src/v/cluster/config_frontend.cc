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

#include "config_frontend.h"

#include "cluster/cluster_utils.h"

namespace cluster {

config_frontend::config_frontend(
  ss::sharded<controller_stm>& stm, ss::sharded<ss::abort_source>& as)
  : _stm(stm)
  , _as(as) {}

ss::future<> config_frontend::start() { return ss::now(); }
ss::future<> config_frontend::stop() { return ss::now(); }

ss::future<std::error_code> config_frontend::patch(
  config_update& update, model::timeout_clock::time_point timeout) {
    auto data = cluster_config_delta_cmd_data();

    data.upsert = std::move(update.upsert);
    data.remove = std::move(update.remove);

    auto cmd = cluster_config_delta_cmd(data, 0);
    return replicate_and_wait(_stm, _as, std::move(cmd), timeout);
}

} // namespace cluster