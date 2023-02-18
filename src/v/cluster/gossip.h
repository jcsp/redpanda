// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

// TODO: think about which maps have well bounded sizes and can be flat hash
// maps
#include "absl/container/node_hash_map.h"
#include "model/timeout_clock.h"
#include "bytes/iobuf.h"
#include "model/metadata.h"
#include "rpc/connection_cache.h"
#include "seastar/core/lowres_clock.hh"
#include "seastar/core/sharded.hh"
#include "seastar/core/smp.hh"
#include "serde/serde.h"
#include "utils/named_type.h"
#include "fmt/core.h"

#include <boost/functional/hash.hpp>

#include <functional>
#include <map>
#include <unordered_map>

namespace cluster {

// On named artifact that a particular shard shares out, such as "partitions"
using gossip_item_name = named_type<ss::sstring, struct gossip_item_name_tag>;

// TODO: perhaps this should use node UUID instead of ID, so that gossip
// can work on nodes before they have got a node ID?
class global_shard_id
  : public serde::
      envelope<global_shard_id, serde::version<0>, serde::compat_version<0>> {
public:
    global_shard_id(model::node_id n, ss::shard_id s)
      : node(n)
      , shard(s) {}

    global_shard_id() = default;

    model::node_id node{model::unassigned_node_id};
    ss::shard_id shard{ss::shard_id{0xffff}};

    void serde_write(iobuf& out) const {
        serde::write(out, static_cast<uint16_t>(node));
        serde::write(out, static_cast<uint16_t>(shard));
    }

    void serde_read(iobuf_parser& in, const serde::header&) {
        node = model::node_id{serde::read_nested<uint16_t>(in, 0)};
        shard = ss::shard_id{serde::read_nested<uint16_t>(in, 0)};
    }

    auto operator<=>(const global_shard_id&) const = default;
    bool operator==(const global_shard_id&) const = default;
};

} // namespace cluster

namespace std {
template<>
struct hash<cluster::global_shard_id> {
    std::size_t operator()(const cluster::global_shard_id& sid) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<uint16_t>()(sid.node));
        boost::hash_combine(h, std::hash<uint16_t>()(sid.shard));
        return h;
    }
};

} // namespace std

namespace cluster {

class gossip_version
  : public serde::
      envelope<gossip_version, serde::version<0>, serde::compat_version<0>> {
private:
    gossip_version(ss::lowres_system_clock::time_point _t)
      : t(_t.time_since_epoch().count()) {}

public:
    gossip_version() { *this = null(); };

    // TODO: a more robust version:
    // - include controller term and controller high offset
    // - use a monotonic clock + the walltime start time of the node
    // - maybe (ab)use the offset of a kvstore partition as a logical clock
    //   for a local node?
    // (but just using system clock works plenty fine for a prototype)
    uint64_t t{0};

    void serde_write(iobuf& out) const {
        serde::write(out, t);
    }

    void serde_read(iobuf_parser& in, const serde::header&) {
        t = serde::read_nested<uint64_t>(in, 0);
    }

    bool operator==(const gossip_version& rhs) const { return rhs.t == t; }

    bool operator>(const gossip_version& rhs) const { return t > rhs.t; }

    static gossip_version now() {
        return gossip_version(ss::lowres_system_clock::now());
    }

    // The null version compares lower than all valid versions
    static gossip_version null() {
        return gossip_version(ss::lowres_system_clock::from_time_t(0));
    }
};

/**
 * The version vector for one shard.
 */
class gossip_version_vector_shard
  : public serde::envelope<
      gossip_version_vector_shard,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    auto serde_fields() { return std::tie(items); }
    std::unordered_map<gossip_item_name, gossip_version> items;
};

// TODO: a more efficient encoding, don't make every nested structure
// a serde::envelope.
/**
 * The version vector for a collection of shards (this might be
 * all the shards on one node in a heartbeat, or it might be
 * all the shards in the whole cluster)
 */
class gossip_version_vector
  : public serde::envelope<
      gossip_version_vector,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    auto serde_fields() { return std::tie(shards); }
    std::unordered_map<global_shard_id, gossip_version_vector_shard> shards;
};

class gossip_state_item : public serde::envelope<gossip_state_item, serde::version<0>, serde::compat_version<0>>{
public:
    gossip_version version;
    iobuf payload;

    auto serde_fields() { return std::tie(version,payload); }
};

/**
 * The state published by a single shard.
 */
class gossip_state {
public:
    gossip_state(model::node_id nid, ss::shard_id sid)
      : node_id(nid)
      , shard_id(sid) {}
    std::map<gossip_item_name, gossip_state_item> items;

    gossip_version_vector get_versions();

    std::optional<std::reference_wrapper<const gossip_state_item>> get_item_full(const gossip_item_name &item) const {
        auto i = items.find(item);
        if (i == items.end()) {
            return std::nullopt;
        } else {
            return std::ref(i->second);
        }
    }

    std::optional<std::reference_wrapper<const iobuf>> get_item(const gossip_item_name &item) const {
        auto r = get_item_full(item);
        if (r.has_value()) {
            return std::ref(r.value().get().payload);
        } else {
            return std::nullopt;
        }
    }

private:
    model::node_id node_id{model::unassigned_node_id};
    [[maybe_unused]] ss::shard_id shard_id{ss::shard_id{0xffff}};
};

/**
 * All states from all nodes: this is collected on shard 0 of each node.  This
 * is a shortcut to implementing gossip simply: in future, this should be
 * replaced by sharing the burden of storing cluster state across all local
 * shards.  That doesn't affect the inter-node scaling, but it avoids
 * concentrating load on shard 0.
 */
class gossip_cluster_state {
public:
    std::map<global_shard_id, gossip_state> shards;
};

class gossip_available_versions_item {
public:
    gossip_version latest_version{gossip_version::null()};

    // There is always at least one item in this set, the node that told
    // us about the version.
    std::set<model::node_id> have_latest_version;
};

/**
 * For a given shard, the global version information (i.e. the latest version
 * known, and which peers have reported the availability of that version and
 * are therefor elegible to serve pull RPCs for the version)
 */
class gossip_available_versions_shard {
public:
    std::map<gossip_item_name, gossip_available_versions_item> items;
};

class gossip_available_versions {
public:
    std::map<global_shard_id, gossip_available_versions_shard> shards;
};

/**
 * The gossip service sends and receives updates about state that all
 * shards on all nodes publish, and pull from one another.
 *
 * Shard 0 collects all the latest states from peers, and is responsible
 * for gathering the vector of state versions from other shards.
 *
 * States are kept as serialized iobufs: this reduces serialization overhead
 * when multiple peers fetch them, and also enables making this class generic:
 * it knows nothing about the data it shares.
 *
 * Each shard may publish multiple states, identified by string names.  Each
 * state has an independent version.  Remote nodes have a choice about which
 * states they choose to fetch from which cores, and how often.  For example
 * larger O(n_partitions) states might be fetched less eagerly than tiny
 * structs that describe a node's disk/memory status.
 */
class gossip : public ss::peering_sharded_service<gossip> {
public:
    explicit gossip(model::node_id nid)
      : this_shard_state(nid, ss::this_shard_id())
      , node_id(nid) {}

    // Get a full report of versions of all items on all shards, suitable
    // for broadcasting over the network to peers.
    ss::future<gossip_version_vector> get_all_versions();

    // On any shard: publish a new version of a gossip item.  This will
    // store the item on this shard, and add it to the vector of available
    // versions that shard 0 will shard with peers on the next heartbeat.
    ss::future<> publish_item(gossip_item_name, iobuf);

    // On shard 0: a local peer notifies us that they have a new version
    // available
    ss::future<>
      notify_local_version(ss::shard_id, gossip_version_vector_shard);

    // On shard 0: a remote node notifies us of their available versions
    ss::future<> notify_remote_versions(
      model::node_id advertizing_node, gossip_version_vector);

    // On shard 0: fetch state from any shard in the cluster
    ss::future<std::optional<std::reference_wrapper<const iobuf>>> get_shard_item(
      const global_shard_id&, const gossip_item_name&);

    ss::future<std::optional<std::reference_wrapper<const gossip_state_item>>> get_shard_item_full(
      const global_shard_id&, const gossip_item_name&);

    // On shard 0: fetch state, waiting up to `timeout` if we don't have it yet
    ss::future<std::reference_wrapper<const iobuf>> await_shard_item(
      const global_shard_id&, const gossip_item_name&,
                                  model::timeout_clock::duration);

    // On shard 0: list the versions that we have heard about but not
    // pulled yet.
    gossip_available_versions get_pull_schedule();

    // On shard 0: having pulled a version from a peer, load it into
    // our local copy of the remote state
    void pulled_remote_item(global_shard_id, const gossip_item_name &, gossip_state_item&&);

    ss::future<> stop() {_as.request_abort(); return ss::now();}

    model::node_id get_node_id() const {return node_id;}

private:
    ss::abort_source _as;

    gossip_version_vector get_this_shard_versions();

    std::optional<gossip_version> peek_version(const global_shard_id &, const gossip_item_name&);

    // The state belonging to this shard, which we shall share out with the
    // world
    gossip_state this_shard_state;

    model::node_id node_id{model::unassigned_node_id};

    // On shard 0: all the state we have from peers
    gossip_cluster_state global_state;

    // On shard 0: all the versions that we have heard about from peers
    gossip_available_versions global_versions;
};


} // namespace cluster


namespace fmt {

template<>
struct formatter<cluster::gossip_version> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template<typename FormatContext>
    typename FormatContext::iterator
    format(const cluster::gossip_version& i, FormatContext& ctx) const;
};

template<>
struct formatter<cluster::global_shard_id> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template<typename FormatContext>
    typename FormatContext::iterator
    format(const cluster::global_shard_id& i, FormatContext& ctx) const;
};

} // namespace fmt
