// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/gossip.h"

#include "cluster/logger.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

namespace fmt {

template<typename FormatContext>
typename FormatContext::iterator
formatter<cluster::gossip_version>::format(const cluster::gossip_version& i, FormatContext& ctx) const {
    return format_to(
      ctx.out(), "gossip_version<{}>", i.t);
}


template<typename FormatContext>
typename FormatContext::iterator
formatter<cluster::global_shard_id>::format(const cluster::global_shard_id& i, FormatContext& ctx) const {
    return format_to(ctx.out(), "{}/{}", i.node, i.shard);
}

} // namespace fmt

namespace cluster {

gossip_version_vector gossip_state::get_versions() {
    gossip_version_vector_shard r_shard;
    for (const auto& i : items) {
        r_shard.items.emplace(i.first, i.second.version);
    }

    gossip_version_vector r;
    global_shard_id this_shard(node_id, ss::this_shard_id());
    r.shards.emplace(this_shard, r_shard);
    return r;
}

gossip_version_vector gossip::get_this_shard_versions() {
    return this_shard_state.get_versions();
}

ss::future<gossip_version_vector> gossip::get_all_versions() {
    gossip_version_vector r = co_await container().map_reduce0(
      [](gossip& g) { return g.get_this_shard_versions(); },
      gossip_version_vector(),
      [](gossip_version_vector a, gossip_version_vector b) {
          for (const auto& i : b.shards) {
              a.shards.emplace(i.first, i.second);
          }
          return a;
      });

    vlog(clusterlog.trace,
         "gossip: advertising versions:");
    for (const auto &i : r.shards) {
        for (const auto &j : i.second.items) {
            vlog(clusterlog.trace, "  {}/{} {}", i.first, j.first, j.second);
        }
    }

    co_return r;
}

ss::future<> gossip::publish_item(gossip_item_name item_name, iobuf data) {
    auto version = gossip_version::now();
    vlog(
      clusterlog.trace,
      "gossip: publishing {} local version {} ({} bytes)",
      item_name,
      version, data.size_bytes());

    this_shard_state.items[
      item_name] =
      gossip_state_item{.version = version, .payload = std::move(data)};

    auto publishing_shard = ss::this_shard_id();

    co_await container().invoke_on(
      ss::shard_id{0}, [item_name, version, publishing_shard](gossip& g) {
          gossip_version_vector_shard vec;
          vec.items[item_name] = version;

          return g.notify_local_version(publishing_shard, vec);
      });
}

ss::future<> gossip::notify_local_version(
  ss::shard_id publishing_shard, gossip_version_vector_shard v) {
    vassert(
      ss::this_shard_id() == ss::shard_id{0},
      "notify_local_version invoked on wrong shard");

    global_shard_id sid(node_id, publishing_shard);

    // FIXME: not convinced this map handling is right, seems like our updates
    // aren't going anywhere.

    auto& shard_versions = global_versions.shards[sid];
    for (const auto& i : v.items) {
        vlog(
          clusterlog.trace,
          "gossip: {}/{}: new local version {}",
          sid,
          i.first,
          i.second);
        shard_versions.items[
          i.first] =
          gossip_available_versions_item{
            .latest_version = i.second, .have_latest_version = {node_id}};
    }
    co_return;
}

ss::future<> gossip::notify_remote_versions(
  model::node_id advertizing_node, gossip_version_vector v) {
    vassert(
      ss::this_shard_id() == ss::shard_id{0},
      "notify_local_version invoked on wrong shard");

    // TODO: if there are versions that were previously advertised from
    // this node but no longer, then clear them out.  This is needed to avoid
    // issuing bad pulls for a short period after a node restarts.

    for (const auto& s : v.shards) {
        auto& shard_versions = global_versions.shards[s.first];
        for (const auto& i : s.second.items) {
            auto& item_version = shard_versions.items[i.first];
            vlog(
              clusterlog.trace,
              "gossip: {}/{}: incoming version {} (current {}) from peer {}",
              s.first,
              i.first,
              i.second,
              item_version.latest_version,
              advertizing_node);
            if (i.second > item_version.latest_version) {
                // A new version: this is the only peer to advertize that they
                // have it so far.
                item_version.latest_version = i.second;
                item_version.have_latest_version = {advertizing_node};
                vlog(
                  clusterlog.trace,
                  "gossip: {}/{}: new version {} from peer {}",
                  s.first,
                  i.first,
                  i.second,
                  advertizing_node);
            } else if (i.second == item_version.latest_version) {
                // An additional peer advertises that it has this version
                if (!item_version.have_latest_version.contains(advertizing_node)) {
                    item_version.have_latest_version.insert(advertizing_node);
                    vlog(
                      clusterlog.trace,
                      "gossip: {}/{}: version {} additional peer {}",
                      s.first,
                      i.first,
                      item_version.latest_version,
                      advertizing_node);
                }
            } else {
                // Stale version, ignore
                vlog(
                  clusterlog.trace,
                  "gossip: {}/{}: stale version {} (latest {}) from peer {}",
                  s.first,
                  i.first,
                  i.second,
                  item_version.latest_version,
                  advertizing_node);
            }
        }
    }
    co_return;
}


/**
 * IMPORTANT: the iobuf reference is only safe for synchronous use.  Do not
 * hold it past a scheduling point.
 */
ss::future<std::optional<std::reference_wrapper<const gossip_state_item>>> gossip::get_shard_item_full(
  const global_shard_id &shard, const gossip_item_name &item) {
    if (shard.node == node_id) {
        // Local data
        if (shard.shard == ss::this_shard_id()) {
            // This shard
            co_return this_shard_state.get_item_full(item);
        } else {
            // Remote shard
            co_return co_await container().invoke_on(shard.shard, [item=std::move(item)](const gossip &g){
                return g.this_shard_state.get_item_full(item);
            });
        }
    } else {
        // Remote data
        auto i = global_state.shards.find(shard);
        if (i == global_state.shards.end()) {
            co_return std::nullopt;
        }

        auto j = i->second.items.find(item);
        if (j == i->second.items.end()) {
            co_return std::nullopt;
        } else {
            co_return j->second;
        }
    }
}

ss::future<std::optional<std::reference_wrapper<const iobuf>>> gossip::get_shard_item(
  const global_shard_id &shard, const gossip_item_name &item) {
    auto r = co_await get_shard_item_full(shard, item);
    if (r.has_value()) {
       co_return r.value().get().payload;
    } else {
        co_return std::nullopt;
    }

}

ss::future<std::reference_wrapper<const iobuf>> gossip::await_shard_item(
  const global_shard_id &shard, const gossip_item_name &item, model::timeout_clock::duration timeout) {
    auto t_initial = model::timeout_clock::now();
    auto deadline = t_initial + timeout;
    do {
        auto r = co_await get_shard_item(shard, item);
        if (r.has_value()) {
            co_return r.value();
        } else {
            // TODO: couple sleep time to the frequency with which we pull updates
            // and/or implement a system of waiters with condition vars.
            co_await ss::sleep_abortable(100ms, _as);
        }

    } while(model::timeout_clock::now() < deadline);

    throw ss::timed_out_error();
}

std::optional<gossip_version> gossip::peek_version(const global_shard_id &shard, const gossip_item_name&item) {
    vassert(shard.node != node_id, "Not for use on self");

    auto i = global_state.shards.find(shard);
    if (i == global_state.shards.end()) {
        return std::nullopt;
    }

    auto j = i->second.items.find(item);
    if (j == i->second.items.end()) {
        return std::nullopt;
    }

    return j->second.version;
}

// FIXME: pull from a random node, not always the home node
gossip_available_versions gossip::get_pull_schedule() {
    gossip_available_versions result;

    for (const auto &i: global_versions.shards) {
        if (i.first.node == node_id) {
            // Never try to pull versions from our local shards: they can be
            // fetched directly in get_shard_item
            continue;
        }

        for (const auto &j : i.second.items) {
            const auto &latest_version = j.second.latest_version;
            const auto have_version = peek_version(i.first, j.first);

            if (!have_version.has_value() || latest_version > have_version.value()) {
                if (have_version.has_value()) {
                    vlog(
                      clusterlog.trace,
                      "gossip: scheduling pull of {}/{} (have={}, want={})",
                      i.first,
                      j.first,
                      have_version.value(),
                      latest_version);
                } else {
                    vlog(
                      clusterlog.trace,
                      "gossip: scheduling pull of {}/{} (have=none, want={})",
                      i.first,
                      j.first,
                      latest_version);
                }
                result.shards[i.first].items[j.first] = j.second;
            }
        }
    }

    return result;
}

void gossip::pulled_remote_item(global_shard_id shard, const gossip_item_name &item_name, gossip_state_item &&item) {
    vlog(clusterlog.trace, "gossip: pulled item {}/{} (version {}, {} bytes)", shard, item_name, item.version, item.payload.size_bytes());
    auto i = global_state.shards.find(shard);
    if (i == global_state.shards.end()) {
        std::tie(i, std::ignore) = global_state.shards.emplace(shard, gossip_state(shard.node, shard.shard));
    }

    auto j = i->second.items.find(item_name);
    if (j == i->second.items.end()) {
        i->second.items.emplace(item_name, std::move(item));
    } else {
        j->second = std::move(item);
    }
}

} // namespace cluster