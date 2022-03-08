/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/fwd.h"
#include "cluster/types.h"
#include "rpc/types.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct feature_barrier_tag_state {
    feature_barrier_tag tag;
    absl::flat_hash_map<model::node_id, bool> nodes_entered;

    // Have we passed through or pre-emptively cancelled the barrier?
    bool exited{false};

    bool is_node_entered(model::node_id nid) {
        auto i = nodes_entered.find(nid);
        if (i == nodes_entered.end()) {
            return false;
        } else {
            return i->second;
        }
    }

    void complete() {
        exited = true;
        exit_wait.signal();
    }

    ss::condition_variable exit_wait;
};

/**
 * State for a one-shot tagged barrier.
 *
 * Sometimes, we want to wait not only for consensus on the state of
 * a feature, but for all nodes to have seen that consensus result (i.e.
 * for the feature_table on all nodes to reflect the new state).
 *
 * Consider a data migration in the `preparing` state, where nodes will stop
 * writing to the old data location once in `preparing`: to be sure that there
 * will be no more writes to the old data, we need to check that all peers
 * have seen the preparing state.
 *
 * Subsequently, before cleaning up some old data, we might want to barrier
 * on all nodes seeing the `active` state.
 */
class feature_barrier_state {
public:
    using rpc_fn_ret
      = ss::future<result<rpc::client_context<feature_barrier_response>>>;
    using rpc_fn = std::function<rpc_fn_ret(
      model::node_id, model::node_id, feature_barrier_tag, bool)>;

    feature_barrier_state(
      model::node_id self,
      ss::sharded<members_table>& members,
      ss::sharded<ss::abort_source>& as,
      ss::gate& gate,
      rpc_fn fn)
      : _members(members)
      , _as(as)
      , _gate(gate)
      , _self(self)
      , _rpc_hook(fn) {}

    ss::future<> barrier(feature_barrier_tag tag);

    void exit_barrier(feature_barrier_tag tag) {
        _barrier_state[tag] = {.tag = tag, .exited = true};
    }

    std::pair<bool, bool>
    update_barrier(feature_barrier_tag tag, model::node_id peer, bool entered);

    /**
     * Test helper.
     */
    const feature_barrier_tag_state& peek_state(feature_barrier_tag tag) {
        return _barrier_state[tag];
    }

private:
    ss::sharded<members_table>& _members;
    ss::sharded<ss::abort_source>& _as;
    ss::gate& _gate;

    std::map<feature_barrier_tag, feature_barrier_tag_state> _barrier_state;

    model::node_id _self;

    rpc_fn _rpc_hook;

    ss::abort_source::subscription _abort_sub;
};

} // namespace cluster