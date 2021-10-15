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

#include "config_manager.h"

#include "cluster/errc.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace cluster {

ss::future<> config_manager::start() { return ss::now(); }
ss::future<> config_manager::stop() { return ss::now(); }

bool config_manager::is_batch_applicable(const model::record_batch& b) {
    return b.header().type == model::record_batch_type::cluster_config_delta_cmd
           || b.header().type
                == model::record_batch_type::cluster_config_status_cmd;
}

ss::future<std::error_code>
config_manager::apply_delta(cluster_config_delta_cmd&& cmd) {
    const auto data = cmd.key;
    vlog(
      clusterlog.trace,
      "apply_delta: {} upserts, {} removes",
      data.upsert.size(),
      data.remove.size());
    for (const auto& u : data.upsert) {
        vlog(clusterlog.trace, "apply_delta: upsert {}={}", u.first, u.second);
    }

    for (const auto& d : data.remove) {
        vlog(clusterlog.trace, "apply_delta: delete {}", d);
    }

    co_await ss::smp::invoke_on_all([&data] {
        auto& cfg = config::shard_local_cfg();
        for (const auto& u : data.upsert) {
            auto& val_yaml = u.second;
            auto val = YAML::Load(val_yaml);
            cfg.get(u.first).set_value(val);
        }

        for (const auto& u : data.remove) {
            auto& property = cfg.get(u);
            property.reset();
        }
    });

    co_return errc::success;
}

ss::future<std::error_code>
config_manager::apply_status(cluster_config_status_cmd&& cmd) {
    auto node_id = cmd.key;
    auto data = cmd.value;

    // TODO: hook into node decom to remove nodes from
    // the status map

    vlog(clusterlog.trace, "apply_status: updating node {}", node_id);

    status[node_id] = data.status;

    co_return errc::success;
}

ss::future<std::error_code>
config_manager::apply_update(model::record_batch b) {
    vlog(clusterlog.trace, "apply_update");

    auto cmd_var = co_await cluster::deserialize(
      std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd_var,
      [this](cluster_config_delta_cmd cmd) {
          return apply_delta(std::move(cmd));
      },
      [this](cluster_config_status_cmd cmd) {
          return apply_status(std::move(cmd));
      });
}

} // namespace cluster
