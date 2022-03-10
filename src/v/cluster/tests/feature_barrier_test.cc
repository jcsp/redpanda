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

#include "cluster/feature_manager.h"
#include "cluster/members_table.h"
#include "test_utils/fixture.h"
#include "vlog.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;
using namespace cluster;

// Variant for unit testing, using manual_clock
using feature_barrier_state_test = feature_barrier_state<ss::manual_clock>;

static ss::logger logger("test");

struct node_state {
    feature_barrier_state_test barrier_state;
    ss::sharded<ss::abort_source> as;
    ss::gate gate;

    node_state(
      ss::sharded<members_table>& members,
      model::node_id id,
      feature_barrier_state_test::rpc_fn fn)
      : barrier_state(id, members, as, gate, fn) {
        as.start().get();
    }

    ~node_state() {
        as.local().request_abort();
        gate.close().get();
        as.stop().get();
    }
};

struct barrier_fixture {
    barrier_fixture() { members.start().get(); }

    ~barrier_fixture() { members.stop().get(); }

    /**
     * Populate members_table with `n` brokers
     */
    void create_brokers(int n) {
        std::vector<model::broker> brokers;
        for (int i = 0; i < n; ++i) {
            brokers.push_back(model::broker(
              model::node_id{i},
              net::unresolved_address{},
              net::unresolved_address{},
              std::nullopt,
              model::broker_properties{}));
        }
        members
          .invoke_on_all([&brokers](members_table& mt) {
              mt.update_brokers(model::offset{0}, brokers);
          })
          .get();
    }

    void create_node_state(model::node_id id) {
        using namespace std::placeholders;

        states[id] = ss::make_lw_shared<node_state>(
          members,
          id,
          std::bind(&barrier_fixture::rpc_hook, this, _1, _2, _3, _4));
    }

    void kill(model::node_id id) { states.erase(id); }
    void restart(model::node_id id) {
        kill(id);
        create_node_state(id);
    }

    feature_barrier_state_test::rpc_fn_ret rpc_hook(
      model::node_id src,
      model::node_id dst,
      feature_barrier_tag tag,
      bool src_entered) {
        if (rpc_rx_errors.contains(dst)) {
            co_return rpc_rx_errors[dst];
        }
        if (rpc_tx_errors.contains(src)) {
            co_return rpc_tx_errors[src];
        }

        BOOST_REQUIRE(states.contains(dst));
        BOOST_REQUIRE(states.contains(src));

        auto [entered, complete] = states[dst]->barrier_state.update_barrier(
          tag, src, src_entered);

        auto client_context = rpc::client_context<feature_barrier_response>(
          rpc::header{}, {.entered = entered, .complete = complete});
        co_return result<rpc::client_context<feature_barrier_response>>(
          std::move(client_context));
    }

    ss::lw_shared_ptr<node_state> get_node_state(model::node_id id) {
        auto r = states[id];
        assert(r);
        return r;
    }

    feature_barrier_state_test& get_barrier_state(model::node_id id) {
        return get_node_state(id)->barrier_state;
    }

    /**
     * Call this in places where the test wishes to advance
     * to the next I/O wait.
     */
    void drain_tasks() { ss::sleep(10ms).get(); }

    std::map<model::node_id, ss::lw_shared_ptr<node_state>> states;

    std::map<model::node_id, std::error_code> rpc_rx_errors;
    std::map<model::node_id, std::error_code> rpc_tx_errors;

    ss::sharded<ss::abort_source> as;
    ss::gate gate;
    ss::sharded<members_table> members;
};

/**
 * The no-op case for a single node cluster
 */
FIXTURE_TEST(test_barrier_single_node, barrier_fixture) {
    create_brokers(1);
    create_node_state(model::node_id{0});

    // Should proceed immediately as it is the only node.
    auto f = get_barrier_state(model::node_id{0})
               .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(f.available() && !f.failed());
};

/**
 * The simple case where everyone enters the barrier and proceeds
 * cleanly to completion.
 */
FIXTURE_TEST(test_barrier_simple, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f1.available());

    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    ss::sleep(10ms).get();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(f2.available());

    // Now that all three have started running, all should complete
    f0.get();
    f1.get();
    f2.get();
}

FIXTURE_TEST(test_barrier_node_restart, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    // Nodes 1+2 enter the barrier
    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});

    BOOST_REQUIRE(!f0.available());
    BOOST_REQUIRE(!f1.available());

    // Node 3 enters the barrier
    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    restart(model::node_id{2});
    f2 = get_barrier_state(model::node_id{2})
           .barrier(feature_barrier_tag{"test"});

    // Prompt reactor to process outstanding futures
    ss::sleep(10ms).get();

    BOOST_REQUIRE(f2.available());

    // All futures are ready.
    f0.get();
    f1.get();
    f2.get();
}

FIXTURE_TEST(test_barrier_node_isolated, barrier_fixture) {
    create_brokers(3);
    create_node_state(model::node_id{0});
    create_node_state(model::node_id{1});
    create_node_state(model::node_id{2});

    rpc_rx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);
    rpc_tx_errors[model::node_id{2}] = cluster::make_error_code(
      cluster::errc::timeout);

    auto f0 = get_barrier_state(model::node_id{0})
                .barrier(feature_barrier_tag{"test"});
    auto f1 = get_barrier_state(model::node_id{1})
                .barrier(feature_barrier_tag{"test"});
    auto f2 = get_barrier_state(model::node_id{2})
                .barrier(feature_barrier_tag{"test"});

    // Without comms to node 2, this barrier should block to complete
    ss::sleep(10ms).get();

    // Advance clock long enough for them to retry and still see an error
    vlog(logger.debug, "Should have just tried first time and failed");
    ss::manual_clock::advance(1000ms);
    ss::sleep(10ms).get();
    vlog(logger.debug, "Should have just retried and failed again");

    rpc_rx_errors.clear();
    rpc_tx_errors.clear();

    // Advance clock far enough for the nodes to all retry (and succeed) their
    // RPCs
    ss::manual_clock::advance(1000ms);
    ss::sleep(10ms).get();
    vlog(logger.debug, "Should have just retried and succeeded");

    BOOST_REQUIRE(f0.available());
    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(f2.available());

    f0.get();
    f1.get();
    f2.get();
}
