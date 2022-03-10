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

#include "feature_barrier.h"

#include "cluster/feature_manager.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "vassert.h"
#include "vlog.h"

namespace cluster {

ss::future<> feature_barrier_state_base::barrier(feature_barrier_tag tag) {
    vassert(
      ss::this_shard_id() == feature_manager::backend_shard,
      "Called barrier on wrong shard");

    vlog(clusterlog.debug, "barrier enter [{}] ({})", _self, tag);

    if (!_members.local().contains(_self)) {
        vlog(
          clusterlog.debug,
          "waiting for cluster membership for barrier ({})",
          tag);
        co_await _members.local().await_membership(_self, _as.local());
    }

    if (_members.local().all_broker_ids().size() < 2) {
        // We are alone, immediate complete.
        vlog(clusterlog.debug, "barrier exit {} (single node)", tag);
        co_return;
    }

    auto gate_holder = _gate.hold();

    // Set our entered state, so that any peers sending feature_barrier
    // requests will see it.
    update_barrier(tag, _self, true);

    // Announce to peers.  Iterate until we have successfully communicated
    // with all peers.
    while (true) {
        bool all_sent = true;
        const auto& member_table = _members.local();
        std::set<model::node_id> sent_to;
        for (const auto& member_id : member_table.all_broker_ids()) {
            if (member_id == _self) {
                // Don't try and send to self
                continue;
            }

            // Check early exit conditions before each RPC
            _as.local().check();

            // We only need to send an RPC to each peer once.  If they restart
            // during this time, they will do an RPC to us and get our readiness
            // in that way.
            if (sent_to.contains(member_id)) {
                vlog(
                  clusterlog.trace,
                  "barrier {} skipping peer {}, already communicated",
                  tag,
                  member_id);
                continue;
            }

            auto rpc_result = co_await _rpc_hook(_self, member_id, tag, true);

            if (rpc_result.has_error()) {
                // Throw abort_requested if the error occurred during shutdown
                _as.local().check();

                // Only at debug level because errors are totally expected
                // when cluster is in the middle of e.g. a rolling restart.
                auto& err = rpc_result.error();
                vlog(
                  clusterlog.debug,
                  "barrier exception sending from {} to {}: {}",
                  _self,
                  member_id,
                  err);

                // Proceed to next node, and eventual retry of this node
                all_sent = false;
                continue;
            } else {
                auto result = rpc_result.value().data;

                sent_to.insert(member_id);

                if (result.complete) {
                    vlog(
                      clusterlog.debug,
                      "barrier {} (peer {} told us complete)",
                      tag,
                      member_id);
                    // Why don't we drop out when someone tells us they
                    // are complete?
                    // Because we must proceed around the loop until I have
                    // successfully communicated with all peers: this is
                    // necessary to ensure that they all know I am ready.
                }

                // Only apply this peer's `entered` if we didn't already
                // enter (prevent race between their RPC to us and our
                // RPC to them).
                if (!_barrier_state[tag].is_node_entered(member_id)) {
                    update_barrier(tag, member_id, result.entered);
                }
            }
        }

        if (all_sent) {
            break;
        } else {
            vlog(
              clusterlog.debug,
              "barrier {} waiting to retry RPCs from node {}",
              tag,
              _self);
            co_await retry_sleep();
        }
    }
    auto& state = _barrier_state.at(tag);
    if (!state.exited) {
        vlog(
          clusterlog.debug,
          "barrier tx complete, waiting ({}) {}",
          tag,
          fmt::ptr(&state.exit_wait));

        _as.local().check();
        auto sub_opt = _as.local().subscribe(
          [&state]() noexcept { state.exit_wait.broken(); });
        co_await state.exit_wait.wait();
    }
    vlog(clusterlog.debug, "barrier exit [{}] ({})", _self, tag);
}

/**
 * Call this when we get an RPC from another node that tells us
 * their barrier state.
 */
std::pair<bool, bool> feature_barrier_state_base::update_barrier(
  feature_barrier_tag tag, model::node_id peer, bool entered) {
    vlog(
      clusterlog.trace,
      "update_barrier [{}] ({}, {}, {})",
      _self,
      tag,
      peer,
      entered);
    auto i = _barrier_state.find(tag);
    if (i == _barrier_state.end()) {
        _barrier_state[tag] = {
          .tag = tag, .nodes_entered = {{peer, entered}}, .exited = false};
        return {false, false};
    } else {
        i->second.nodes_entered[peer] = entered;
        const auto& member_table = _members.local();
        bool all_in = true;
        for (const auto& member_id : member_table.all_broker_ids()) {
            auto j = i->second.nodes_entered.find(member_id);
            if (j == i->second.nodes_entered.end()) {
                vlog(
                  clusterlog.debug,
                  "update_barrier: no state yet for peer {} ({})",
                  member_id,
                  tag);
                all_in = false;
                break;
            } else {
                if (j->second == false) {
                    vlog(
                      clusterlog.debug,
                      "update_barrier: not entered yet peer {} ({})",
                      member_id,
                      tag);
                    all_in = false;
                    break;
                }
            }
        }

        if (all_in && !i->second.exited) {
            vlog(clusterlog.debug, "barrier all in [{}] ({})", _self, tag);
            i->second.complete();
        }

        return {i->second.is_node_entered(_self), i->second.exited};
    }
}
} // namespace cluster