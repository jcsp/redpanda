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

#include "cluster/controller_stm.h"

namespace cluster {

struct config_update final {
    std::vector<std::pair<ss::sstring, ss::sstring>> upsert;
    std::vector<ss::sstring> remove;
};

class config_frontend final {
public:
    config_frontend(
      ss::sharded<controller_stm>&, ss::sharded<ss::abort_source>&);
    ss::future<std::error_code>
    patch(config_update&, model::timeout_clock::time_point);

    ss::future<> start();
    ss::future<> stop();

private:
    ss::sharded<controller_stm>& _stm;
    ss::sharded<ss::abort_source>& _as;
};
} // namespace cluster
