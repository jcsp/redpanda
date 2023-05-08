/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_planner.h"

#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>
#include <seastar/util/defer.hh>

#include <optional>

namespace cluster {

namespace {

hard_constraint
distinct_from(const absl::flat_hash_set<model::node_id>& nodes) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(const absl::flat_hash_set<model::node_id>& nodes)
          : _nodes(nodes) {}

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [this](const allocation_node& node) {
                return !_nodes.contains(node.id());
            };
        }

        ss::sstring name() const final {
            return ssx::sformat(
              "distinct from nodes: {}",
              std::vector(_nodes.begin(), _nodes.end()));
        }

    private:
        const absl::flat_hash_set<model::node_id>& _nodes;
    };

    return hard_constraint(std::make_unique<impl>(nodes));
}

} // namespace

partition_balancer_planner::partition_balancer_planner(
  planner_config config,
  partition_balancer_state& state,
  partition_allocator& partition_allocator)
  : _config(config)
  , _state(state)
  , _partition_allocator(partition_allocator) {
    _config.soft_max_disk_usage_ratio = std::min(
      _config.soft_max_disk_usage_ratio, _config.hard_max_disk_usage_ratio);
}

class partition_balancer_planner::request_context {
public:
    std::vector<model::node_id> all_nodes;
    absl::flat_hash_set<model::node_id> all_unavailable_nodes;
    absl::flat_hash_set<model::node_id> timed_out_unavailable_nodes;
    size_t num_nodes_in_maintenance = 0;
    absl::flat_hash_set<model::node_id> decommissioning_nodes;
    absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;

    absl::flat_hash_map<model::ntp, size_t> ntp_sizes;

    absl::btree_map<model::ntp, allocated_partition> reassignments;
    size_t failed_reassignments_count = 0;
    uint64_t planned_moves_size = 0;
    absl::btree_set<model::ntp> cancellations;

    bool is_batch_full() const {
        return planned_moves_size >= _parent._config.movement_disk_size_batch;
    }

    void for_each_partition(
      ss::noncopyable_function<ss::stop_iteration(partition&)>);
    void with_partition(
      const model::ntp&, ss::noncopyable_function<void(partition&)>);

    bool is_partition_movement_possible(
      const std::vector<model::broker_shard>& current_replicas) const;

    allocation_constraints get_allocation_constraints(
      size_t partition_size, double max_disk_usage_ratio) const;

private:
    friend class partition_balancer_planner;

    request_context(partition_balancer_planner& parent)
      : _parent(parent) {}

    bool all_reports_received() const;

    template<typename Visitor>
    auto do_with_partition(
      const model::ntp& ntp,
      const std::vector<model::broker_shard>& orig_replicas,
      Visitor&);

    void collect_actions(plan_data&);

private:
    partition_balancer_planner& _parent;
};

void partition_balancer_planner::init_per_node_state(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics,
  request_context& ctx,
  plan_data& result) const {
    for (const auto& [id, broker] : _state.members().nodes()) {
        if (
          broker.state.get_membership_state()
          == model::membership_state::removed) {
            continue;
        }

        ctx.all_nodes.push_back(id);

        if (
          broker.state.get_maintenance_state()
          == model::maintenance_state::active) {
            vlog(clusterlog.debug, "node {}: in maintenance", id);
            ctx.num_nodes_in_maintenance += 1;
        }

        if (
          broker.state.get_membership_state()
          == model::membership_state::draining) {
            vlog(clusterlog.debug, "node {}: decommissioning", id);
            ctx.decommissioning_nodes.insert(id);
        }
    }

    const auto now = raft::clock_type::now();
    for (const auto& follower : follower_metrics) {
        auto unavailable_dur = now - follower.last_heartbeat;

        vlog(
          clusterlog.debug,
          "node {}: {} ms since last heartbeat",
          follower.id,
          std::chrono::duration_cast<std::chrono::milliseconds>(unavailable_dur)
            .count());

        if (follower.is_live) {
            continue;
        }

        ctx.all_unavailable_nodes.insert(follower.id);

        if (unavailable_dur > _config.node_availability_timeout_sec) {
            ctx.timed_out_unavailable_nodes.insert(follower.id);
            model::timestamp unavailable_since = model::to_timestamp(
              model::timestamp_clock::now()
              - std::chrono::duration_cast<model::timestamp_clock::duration>(
                unavailable_dur));
            result.violations.unavailable_nodes.emplace_back(
              follower.id, unavailable_since);
        }
    }

    for (const auto& node_report : health_report.node_reports) {
        const uint64_t total = node_report.local_state.data_disk.total;
        const uint64_t free = node_report.local_state.data_disk.free;

        ctx.node_disk_reports.emplace(
          node_report.id, node_disk_space(node_report.id, total, total - free));
    }

    for (const auto& [id, disk] : ctx.node_disk_reports) {
        double used_space_ratio = disk.original_used_ratio();
        vlog(
          clusterlog.debug,
          "node {}: bytes used: {}, bytes total: {}, used ratio: {:.4}",
          id,
          disk.used,
          disk.total,
          used_space_ratio);
        if (used_space_ratio > _config.soft_max_disk_usage_ratio) {
            result.violations.full_nodes.emplace_back(
              id, uint32_t(used_space_ratio * 100.0));
        }
    }
}

void partition_balancer_planner::init_ntp_sizes_from_health_report(
  const cluster_health_report& health_report, request_context& ctx) {
    for (const auto& node_report : health_report.node_reports) {
        for (const auto& tp_ns : node_report.topics) {
            for (const auto& partition : tp_ns.partitions) {
                ctx.ntp_sizes[model::ntp(
                  tp_ns.tp_ns.ns, tp_ns.tp_ns.tp, partition.id)]
                  = partition.size_bytes;
            }
        }
    }
}

bool partition_balancer_planner::request_context::all_reports_received() const {
    for (auto id : all_nodes) {
        if (
          !all_unavailable_nodes.contains(id)
          && !node_disk_reports.contains(id)) {
            vlog(clusterlog.info, "No disk report for node {}", id);
            return false;
        }
    }
    return true;
}

bool partition_balancer_planner::request_context::
  is_partition_movement_possible(
    const std::vector<model::broker_shard>& current_replicas) const {
    // Check that nodes quorum is available
    size_t available_nodes_amount = std::count_if(
      current_replicas.begin(),
      current_replicas.end(),
      [this](const model::broker_shard& bs) {
          return !all_unavailable_nodes.contains(bs.node_id);
      });
    if (available_nodes_amount * 2 < current_replicas.size()) {
        return false;
    }
    return true;
}

std::optional<size_t> partition_balancer_planner::get_partition_size(
  const model::ntp& ntp, const request_context& ctx) {
    const auto ntp_data = ctx.ntp_sizes.find(ntp);
    if (ntp_data == ctx.ntp_sizes.end()) {
        vlog(
          clusterlog.info,
          "Partition {} status was not found in cluster health "
          "report",
          ntp);
    } else {
        return ntp_data->second;
    }
    return std::nullopt;
}

allocation_constraints
partition_balancer_planner::request_context::get_allocation_constraints(
  size_t partition_size, double max_disk_usage_ratio) const {
    allocation_constraints constraints;

    // Add constraint on least disk usage
    constraints.add(least_disk_filled(max_disk_usage_ratio, node_disk_reports));

    // Add constraint on partition max_disk_usage_ratio overfill
    size_t upper_bound_for_partition_size
      = partition_size + _parent._config.segment_fallocation_step;
    constraints.add(disk_not_overflowed_by_partition(
      max_disk_usage_ratio, upper_bound_for_partition_size, node_disk_reports));

    // Add constraint on unavailable nodes
    constraints.add(distinct_from(timed_out_unavailable_nodes));

    // Add constraint on decommissioning nodes
    if (!decommissioning_nodes.empty()) {
        constraints.add(distinct_from(decommissioning_nodes));
    }

    return constraints;
}

result<model::broker_shard> partition_balancer_planner::move_replica(
  const model::ntp& ntp,
  allocated_partition& allocated,
  size_t partition_size,
  model::node_id previous,
  allocation_constraints constraints,
  std::string_view reason,
  request_context& ctx) {
    vlog(
      clusterlog.debug,
      "trying to move replica {} of ntp {} (size: {}, current replicas: {}), "
      "reason: {}",
      previous,
      ntp,
      partition_size,
      allocated.replicas(),
      reason);

    auto moved = _partition_allocator.reallocate_replica(
      allocated, previous, std::move(constraints));

    if (!moved) {
        vlog(
          clusterlog.info,
          "attempt to move replica {} for ntp {} (reason: {}) failed, error: "
          "{}",
          previous,
          ntp,
          reason,
          moved.error().message());

        return moved.error();
    }

    if (moved.value().node_id != previous) {
        auto from_it = ctx.node_disk_reports.find(previous);
        if (from_it != ctx.node_disk_reports.end()) {
            from_it->second.released += partition_size;
        }

        auto to_it = ctx.node_disk_reports.find(moved.value().node_id);
        if (to_it != ctx.node_disk_reports.end()) {
            to_it->second.assigned += partition_size;
        }
    }

    return moved;
}

class partition_balancer_planner::partition {
public:
    const model::ntp& ntp() const { return _ntp; }
    const std::vector<model::broker_shard>& replicas() const {
        return (_reallocated ? _reallocated->replicas() : _orig_replicas);
    };

    bool is_reassignment_possible() const { return _is_reassignment_possible; }

    bool is_original(const model::broker_shard& replica) const {
        return !_reallocated || _reallocated->is_original(replica);
    }

    std::optional<size_t> size_bytes() const { return _size_bytes; }

    result<model::broker_shard> move_replica(
      model::node_id replica,
      double max_disk_usage_ratio,
      std::string_view reason);

private:
    friend class request_context;

    partition(
      model::ntp ntp,
      std::optional<size_t> size_bytes,
      std::optional<allocated_partition> reallocated,
      const std::vector<model::broker_shard>& orig_replicas,
      request_context& ctx)
      : _ntp(std::move(ntp))
      , _size_bytes(size_bytes)
      , _reallocated(std::move(reallocated))
      , _orig_replicas(orig_replicas)
      , _is_reassignment_possible(
          _size_bytes && ctx.is_partition_movement_possible(orig_replicas))
      , _ctx(ctx) {}

    bool has_changes() const {
        return _reallocated && _reallocated->has_node_changes();
    }

private:
    model::ntp _ntp;
    std::optional<size_t> _size_bytes;
    std::optional<allocated_partition> _reallocated;
    const std::vector<model::broker_shard>& _orig_replicas;
    bool _is_reassignment_possible = false;
    request_context& _ctx;
};

template<typename Visitor>
auto partition_balancer_planner::request_context::do_with_partition(
  const model::ntp& ntp,
  const std::vector<model::broker_shard>& orig_replicas,
  Visitor& visitor) {
    std::optional<allocated_partition> reallocated;
    auto reassignment_it = reassignments.find(ntp);
    if (reassignment_it != reassignments.end()) {
        // borrow the allocated_partition object
        reallocated = std::move(reassignment_it->second);
    }

    std::optional<size_t> size_bytes;
    auto size_it = ntp_sizes.find(ntp);
    if (size_it != ntp_sizes.end()) {
        size_bytes = size_it->second;
    }

    auto part = partition{
      ntp, size_bytes, std::move(reallocated), orig_replicas, *this};
    auto deferred = ss::defer([&] {
        // insert or return part._reallocated to reassignments
        if (reassignment_it != reassignments.end()) {
            reassignment_it->second = std::move(*part._reallocated);
        } else if (part._reallocated && part._reallocated->has_node_changes()) {
            reassignments.emplace(ntp, std::move(*part._reallocated));
            planned_moves_size += part._size_bytes.value();
        }
    });

    return visitor(part);
}

void partition_balancer_planner::request_context::for_each_partition(
  ss::noncopyable_function<ss::stop_iteration(partition&)> callback) {
    for (const auto& t : _parent._state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            auto stop = do_with_partition(ntp, a.replicas, callback);
            if (stop == ss::stop_iteration::yes) {
                return;
            }
        }
    }
}

void partition_balancer_planner::request_context::with_partition(
  const model::ntp& ntp, ss::noncopyable_function<void(partition&)> callback) {
    auto topic = model::topic_namespace_view(ntp);
    auto topic_meta = _parent._state.topics().get_topic_metadata_ref(topic);
    if (!topic_meta) {
        vlog(clusterlog.warn, "topic {} not found", topic);
        return;
    }
    auto it = topic_meta->get().get_assignments().find(ntp.tp.partition);
    if (it == topic_meta->get().get_assignments().end()) {
        vlog(
          clusterlog.warn,
          "partition {} of topic {} not found",
          ntp.tp.partition,
          topic);
        return;
    }

    do_with_partition(ntp, it->replicas, callback);
}

result<model::broker_shard> partition_balancer_planner::partition::move_replica(
  model::node_id replica,
  double max_disk_usage_ratio,
  std::string_view reason) {
    if (!_is_reassignment_possible) {
        return errc::invalid_request;
    }

    if (!_reallocated) {
        _reallocated
          = _ctx._parent._partition_allocator.make_allocated_partition(
            replicas(), get_allocation_domain(_ntp));
    }

    auto constraints = _ctx.get_allocation_constraints(
      _size_bytes.value(), max_disk_usage_ratio);

    auto moved = _ctx._parent.move_replica(
      _ntp, *_reallocated, *_size_bytes, replica, constraints, reason, _ctx);
    if (!moved) {
        _ctx.failed_reassignments_count += 1;
    }
    return moved;
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating soft_max_disk_usage_ratio constraint
 */
void partition_balancer_planner::get_unavailable_nodes_reassignments(
  request_context& ctx) {
    if (ctx.timed_out_unavailable_nodes.empty()) {
        return;
    }

    for (const auto& t : _state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            // End adding movements if batch is collected
            if (ctx.is_batch_full()) {
                return;
            }

            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            if (ctx.reassignments.contains(ntp)) {
                continue;
            }

            std::vector<model::node_id> to_move;
            for (const auto& bs : a.replicas) {
                if (ctx.timed_out_unavailable_nodes.contains(bs.node_id)) {
                    to_move.push_back(bs.node_id);
                }
            }

            if (to_move.empty()) {
                continue;
            }

            auto partition_size = get_partition_size(ntp, ctx);
            if (
              !partition_size.has_value()
              || !ctx.is_partition_movement_possible(a.replicas)) {
                ctx.failed_reassignments_count += 1;
                continue;
            }

            auto reallocated = _partition_allocator.make_allocated_partition(
              a.replicas, get_allocation_domain(ntp));

            auto constraints = ctx.get_allocation_constraints(
              partition_size.value(), _config.hard_max_disk_usage_ratio);

            for (const auto& replica : to_move) {
                auto moved = move_replica(
                  ntp,
                  reallocated,
                  partition_size.value(),
                  replica,
                  constraints,
                  "unavailable nodes",
                  ctx);
                if (!moved) {
                    ctx.failed_reassignments_count += 1;
                }
            }

            if (reallocated.has_node_changes()) {
                ctx.planned_moves_size += partition_size.value();
                ctx.reassignments.emplace(ntp, std::move(reallocated));
            }
        }
    }
}

/// Try to fix ntps that have several replicas in one rack (these ntps can
/// appear because rack awareness constraint is not a hard constraint, e.g. when
/// a rack dies and we move all replicas that resided on dead nodes to live
/// ones).
///
/// We go over all such ntps (a list maintained by partition_balancer_state) and
/// if the number of currently live racks is more than the number of racks that
/// the ntp is replicated to, we try to schedule a move. For each rack we
/// arbitrarily choose the first appearing replica to remain there (note: this
/// is probably not optimal choice).
void partition_balancer_planner::get_rack_constraint_repair_reassignments(
  request_context& ctx) {
    if (_state.ntps_with_broken_rack_constraint().empty()) {
        return;
    }

    absl::flat_hash_set<model::rack_id> available_racks;
    for (auto node_id : ctx.all_nodes) {
        if (!ctx.timed_out_unavailable_nodes.contains(node_id)) {
            auto rack = _state.members().get_node_rack_id(node_id);
            if (rack) {
                available_racks.insert(*rack);
            }
        }
    }

    for (const auto& ntp : _state.ntps_with_broken_rack_constraint()) {
        if (ctx.is_batch_full()) {
            return;
        }

        if (ctx.reassignments.contains(ntp)) {
            continue;
        }

        auto assignment = _state.topics().get_partition_assignment(ntp);
        if (!assignment) {
            vlog(clusterlog.warn, "assignment for ntp {} not found", ntp);
            continue;
        }

        const auto& orig_replicas = assignment->replicas;

        std::vector<model::node_id> to_move;
        absl::flat_hash_set<model::rack_id> cur_racks;
        for (const auto& bs : orig_replicas) {
            auto rack = _state.members().get_node_rack_id(bs.node_id);
            if (rack) {
                auto [it, inserted] = cur_racks.insert(*rack);
                if (!inserted) {
                    to_move.push_back(bs.node_id);
                }
            }
        }

        if (to_move.empty()) {
            continue;
        }

        if (available_racks.size() <= cur_racks.size()) {
            // Can't repair the constraint if we don't have an available rack to
            // place a replica there.
            continue;
        }

        auto partition_size = get_partition_size(ntp, ctx);
        if (
          !partition_size.has_value()
          || !ctx.is_partition_movement_possible(orig_replicas)) {
            ctx.failed_reassignments_count += 1;
            continue;
        }

        auto reallocated = _partition_allocator.make_allocated_partition(
          orig_replicas, get_allocation_domain(ntp));

        auto constraints = ctx.get_allocation_constraints(
          partition_size.value(), _config.hard_max_disk_usage_ratio);

        for (const auto& replica : to_move) {
            auto moved = move_replica(
              ntp,
              reallocated,
              partition_size.value(),
              replica,
              constraints,
              "rack constraint repair",
              ctx);
            if (!moved || moved.value().node_id == replica) {
                ctx.failed_reassignments_count += 1;
            }
        }

        if (reallocated.has_node_changes()) {
            ctx.planned_moves_size += partition_size.value();
            ctx.reassignments.emplace(ntp, std::move(reallocated));
        }
    }
}

/*
 * Function is trying to move ntps out of node that are violating
 * soft_max_disk_usage_ratio. It takes nodes in reverse used space ratio order.
 * For each node it is trying to collect set of partitions to move. Partitions
 * are selected in ascending order of their size.
 *
 * If more than one replica in a group is on a node violating disk usage
 * constraints, we try to reallocate all such replicas. Some of reallocation
 * requests can fail, we just move those replicas that we can.
 */
void partition_balancer_planner::get_full_node_reassignments(
  request_context& ctx) {
    std::vector<const node_disk_space*> sorted_full_nodes;
    for (const auto& kv : ctx.node_disk_reports) {
        const auto* node_disk = &kv.second;
        if (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio) {
            sorted_full_nodes.push_back(node_disk);
        }
    }
    std::sort(
      sorted_full_nodes.begin(),
      sorted_full_nodes.end(),
      [](const auto* lhs, const auto* rhs) {
          return lhs->final_used_ratio() > rhs->final_used_ratio();
      });

    if (sorted_full_nodes.empty()) {
        return;
    }

    absl::flat_hash_map<model::node_id, std::vector<model::ntp>> ntp_on_nodes;
    for (const auto& t : _state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            for (const auto& r : a.replicas) {
                ntp_on_nodes[r.node_id].emplace_back(
                  t.first.ns, t.first.tp, a.id);
            }
        }
    }

    for (const auto* node_disk : sorted_full_nodes) {
        if (ctx.is_batch_full()) {
            return;
        }

        absl::btree_multimap<size_t, model::ntp> ntp_on_node_sizes;
        for (const auto& ntp : ntp_on_nodes[node_disk->node_id]) {
            auto partition_size_opt = get_partition_size(ntp, ctx);
            if (partition_size_opt.has_value()) {
                ntp_on_node_sizes.emplace(partition_size_opt.value(), ntp);
            } else {
                ctx.failed_reassignments_count += 1;
            }
        }

        auto ntp_size_it = ntp_on_node_sizes.begin();
        while (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio
               && ntp_size_it != ntp_on_node_sizes.end()) {
            if (ctx.is_batch_full()) {
                return;
            }

            const auto& partition_to_move = ntp_size_it->second;
            if (ctx.reassignments.contains(partition_to_move)) {
                ntp_size_it++;
                continue;
            }

            const auto& topic_metadata = _state.topics().topics_map().at(
              model::topic_namespace_view(partition_to_move));
            const auto& current_replicas = topic_metadata.get_assignments()
                                             .find(
                                               partition_to_move.tp.partition)
                                             ->replicas;

            if (!ctx.is_partition_movement_possible(current_replicas)) {
                ctx.failed_reassignments_count += 1;
                ntp_size_it++;
                continue;
            }

            struct full_node_replica {
                model::broker_shard bs;
                node_disk_space disk;
            };
            std::vector<full_node_replica> full_node_replicas;

            for (const auto& r : current_replicas) {
                if (ctx.timed_out_unavailable_nodes.contains(r.node_id)) {
                    continue;
                }

                auto disk_it = ctx.node_disk_reports.find(r.node_id);
                if (disk_it == ctx.node_disk_reports.end()) {
                    // A replica on a node we recently lost contact with (but
                    // availability timeout hasn't elapsed yet). Better leave it
                    // where it is.
                    continue;
                }

                const auto& disk = disk_it->second;
                if (
                  disk.final_used_ratio()
                  >= _config.soft_max_disk_usage_ratio) {
                    full_node_replicas.push_back(full_node_replica{
                      .bs = r,
                      .disk = disk,
                    });
                }
            }

            // Try to reallocate replicas starting from the most full node
            std::sort(
              full_node_replicas.begin(),
              full_node_replicas.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.disk.final_used_ratio()
                         > rhs.disk.final_used_ratio();
              });

            auto reallocated = _partition_allocator.make_allocated_partition(
              current_replicas, get_allocation_domain(ntp_size_it->second));

            auto constraints = ctx.get_allocation_constraints(
              ntp_size_it->first, _config.soft_max_disk_usage_ratio);

            for (const auto& replica : full_node_replicas) {
                auto moved = move_replica(
                  ntp_size_it->second,
                  reallocated,
                  ntp_size_it->first,
                  replica.bs.node_id,
                  constraints,
                  "full_nodes",
                  ctx);
                if (!moved) {
                    ctx.failed_reassignments_count += 1;
                }
            }
            if (reallocated.has_node_changes()) {
                ctx.planned_moves_size += ntp_size_it->first;
                ctx.reassignments.emplace(
                  ntp_size_it->second, std::move(reallocated));
            }

            ntp_size_it++;
        }
    }
}

/*
 * Cancel movement if new assignments contains unavailble node
 * and previous replica set doesn't contain this node
 */
void partition_balancer_planner::get_unavailable_node_movement_cancellations(
  request_context& ctx) {
    for (const auto& update : _state.topics().updates_in_progress()) {
        if (update.second.get_state() != reconfiguration_state::in_progress) {
            continue;
        }

        absl::flat_hash_set<model::node_id> previous_replicas_set;
        bool was_on_decommissioning_node = false;
        for (const auto& r : update.second.get_previous_replicas()) {
            previous_replicas_set.insert(r.node_id);
            if (ctx.decommissioning_nodes.contains(r.node_id)) {
                was_on_decommissioning_node = true;
            }
        }

        auto current_assignments = _state.topics().get_partition_assignment(
          update.first);
        if (!current_assignments.has_value()) {
            continue;
        }
        for (const auto& r : current_assignments->replicas) {
            if (
              ctx.timed_out_unavailable_nodes.contains(r.node_id)
              && !previous_replicas_set.contains(r.node_id)) {
                if (!was_on_decommissioning_node) {
                    vlog(
                      clusterlog.info,
                      "ntp: {}, cancelling move {} -> {}",
                      update.first,
                      update.second.get_previous_replicas(),
                      current_assignments->replicas);

                    ctx.cancellations.emplace(update.first);
                } else {
                    ctx.failed_reassignments_count += 1;
                }
                break;
            }
        }
    }
}

void partition_balancer_planner::request_context::collect_actions(
  partition_balancer_planner::plan_data& result) {
    result.reassignments.reserve(reassignments.size());
    for (auto& [ntp, reallocated] : reassignments) {
        result.reassignments.push_back(
          ntp_reassignment{.ntp = ntp, .allocated = std::move(reallocated)});
    }

    result.failed_reassignments_count = failed_reassignments_count;

    result.cancellations.reserve(cancellations.size());
    std::move(
      cancellations.begin(),
      cancellations.end(),
      std::back_inserter(result.cancellations));

    if (!result.cancellations.empty() || !result.reassignments.empty()) {
        result.status = status::actions_planned;
    }
}

partition_balancer_planner::plan_data partition_balancer_planner::plan_actions(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics) {
    request_context ctx(*this);
    plan_data result;

    init_per_node_state(health_report, follower_metrics, ctx, result);

    if (ctx.num_nodes_in_maintenance > 0) {
        if (!result.violations.is_empty()) {
            result.status = status::waiting_for_maintenance_end;
        }
        return result;
    }

    if (_state.topics().has_updates_in_progress()) {
        get_unavailable_node_movement_cancellations(ctx);

        ctx.collect_actions(result);
        return result;
    }

    if (!ctx.all_reports_received()) {
        result.status = status::waiting_for_reports;
        return result;
    }

    if (
      result.violations.is_empty()
      && _state.ntps_with_broken_rack_constraint().empty()) {
        result.status = status::empty;
        return result;
    }

    init_ntp_sizes_from_health_report(health_report, ctx);

    get_unavailable_nodes_reassignments(ctx);
    get_rack_constraint_repair_reassignments(ctx);
    get_full_node_reassignments(ctx);

    ctx.collect_actions(result);
    return result;
}

} // namespace cluster
