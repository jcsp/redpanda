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
    return b.header().type
           == model::record_batch_type::cluster_config_delta_cmd;
}
ss::future<std::error_code>
config_manager::apply_update(model::record_batch b) {
    vlog(clusterlog.trace, "apply_update");

    auto cmd_var = co_await cluster::deserialize(
      std::move(b), accepted_commands);

    const auto cmd = std::get<cluster_config_delta_cmd>(cmd_var);
    const auto data = cmd.key;

    vlog(
      clusterlog.trace,
      "apply_update: {} upserts, {} removes",
      data.upsert.size(),
      data.remove.size());
    for (const auto& u : data.upsert) {
        vlog(clusterlog.trace, "apply_update: upsert {}={}", u.first, u.second);
    }

    for (const auto& d : data.remove) {
        vlog(clusterlog.trace, "apply_update: delete {}", d);
    }

    co_await ss::smp::invoke_on_all([&data] {
        auto& cfg = config::shard_local_cfg();
        //        for (const auto& u : data.upsert) {
        //             cfg.get(u.first).set_value(u.second);
        //        }

        for (const auto& u : data.upsert) {
            auto& val_yaml = u.second;
            auto val = YAML::Load(val_yaml);
            cfg.get(u.first).set_value(val);
        }

        // config_store is built for base_property, but base_property
        // doesn't have a concept of a default.
        //        for (const auto& u : data.remove) {
        //            auto& property = cfg.get(u);
        //            property.set_value(property.default_value());
        //        }
    });

    co_return errc::success;
}

} // namespace cluster
