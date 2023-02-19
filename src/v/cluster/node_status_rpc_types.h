/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/gossip.h"
#include "model/metadata.h"
#include "serde/serde.h"

namespace cluster {

struct node_status_metadata
  : serde::envelope<
      node_status_metadata,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    model::node_id node_id;

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_metadata& nsm) {
        fmt::print(o, "{{node_id:{}}}", nsm.node_id);
        return o;
    }
};

struct node_status_request
  : serde::envelope<
      node_status_request,
      serde::version<1>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    node_status_metadata sender_metadata;

    // TODO: on receiving node_status_request, selectively send versions in
    // node_status_reply to let the requester know what we have that they
    // do not have.
    gossip_version_vector gossip_versions;

    auto serde_fields() { return std::tie(sender_metadata, gossip_versions); }

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_request& r) {
        fmt::print(o, "{{sender_metadata: {}}}", r.sender_metadata);
        return o;
    }
};

struct node_status_reply
  : serde::
      envelope<node_status_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    node_status_metadata replier_metadata;

    friend std::ostream&
    operator<<(std::ostream& o, const node_status_reply& r) {
        fmt::print(o, "{{replier_metadata: {}}}", r.replier_metadata);
        return o;
    }
};

struct gossip_pull_request
 : serde::envelope<gossip_pull_request, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    global_shard_id shard;
    std::vector<gossip_item_name> items;

    auto serde_fields() { return std::tie(shard, items); }

    friend std::ostream&
    operator<<(std::ostream& o, const gossip_pull_request& r) {
        fmt::print(o, "{{gossip_pull_request: shard {}}}", r.shard);
        return o;
    }
};


struct gossip_pull_item
: public serde::envelope<gossip_pull_item, serde::version<0>, serde::compat_version<0>> {
public:
    gossip_item_name name;
    gossip_state_item item;
    auto serde_fields() { return std::tie(name, item); }
};


struct gossip_pull_reply
  : serde::envelope<gossip_pull_reply, serde::version<0>, serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    // TODO: decide how to represent errors.  Data can be not found if the
    // node restarted since the request was sent.  Currently we just omit it
    // in the response.
    std::vector<gossip_pull_item> items;

    auto serde_fields() { return std::tie(items); }

    friend std::ostream&
    operator<<(std::ostream& o, const gossip_pull_reply& r) {
        fmt::print(o, "{{gossip_pull_reply: {} items}}", r.items.size());
        return o;
    }
};

} // namespace cluster
