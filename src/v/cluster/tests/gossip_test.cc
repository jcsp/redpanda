// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE gossip

#include "cluster/gossip.h"

#include <seastar/testing/thread_test_case.hh>

using namespace cluster;

SEASTAR_THREAD_TEST_CASE(collect_versions) {
    ss::sharded<gossip> svc;
    svc.start(model::node_id{0}).get();
    auto result = svc.local().get_all_versions().get();
    svc.stop().get();
}

BOOST_AUTO_TEST_CASE(version_vector_encoding) {
    gossip_version_vector v;
    gossip_version_vector_shard vs;
    vs.items.emplace(gossip_item_name{"ohai"}, gossip_version::now());
    v.shards.emplace(global_shard_id(model::node_id{0xfeed}, ss::shard_id{0xbeef}), std::move(vs));


    auto serde_in = v;
    auto serde_out = serde::to_iobuf(std::move(serde_in));

    std::cerr << serde_out.hexdump(1024) << std::endl;

      auto from_serde = serde::from_iobuf<gossip_version_vector>(std::move(serde_out));
    BOOST_REQUIRE(v == from_serde);
}

BOOST_AUTO_TEST_CASE(version_vector_encoding_empty) {
    gossip_version_vector v;

    auto serde_in = v;
    auto serde_out = serde::to_iobuf(std::move(serde_in));
    auto from_serde = serde::from_iobuf<gossip_version_vector>(std::move(serde_out));
    BOOST_REQUIRE(v == from_serde);
}
