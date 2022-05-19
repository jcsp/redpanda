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

#include "storage_resources.h"

#include "config/configuration.h"
#include "storage/logger.h"
#include "vlog.h"

namespace storage {

storage_resources::storage_resources()
  : _segment_fallocation_step(
    config::shard_local_cfg().segment_fallocation_step.bind())
  // FIXME: I made this needsx_restart::no so that I could bind it, but
  // elsewhere it's used in a way that very much does require a restart.
  , _append_chunk_size(config::shard_local_cfg().append_chunk_size.bind())

{}

void storage_resources::update_allowance(uint64_t total, uint64_t free) {
    // TODO: also take as an input the disk consumption of the SI cache:
    // it knows this because it calculates it when doing periodic trimming.
    if (
      config::shard_local_cfg().cloud_storage_enabled
      && total < config::shard_local_cfg().cloud_storage_cache_size()) {
        total -= config::shard_local_cfg().cloud_storage_cache_size();
    }

    _space_allowance = total;
    _space_allowance_free = std::min(free, total);

    // Reset counter for falloc space consumed between updates.
    _projected_consumption = 0;
    _projected_partitions = 0;
}

size_t
storage_resources::get_falloc_step(std::optional<uint64_t> segment_size_hint) {
    // Heuristic: use at most half the available disk space for per-allocating
    // space to write into.

    // At most, use the configured fallocation step.
    size_t guess = _segment_fallocation_step();

    if (_partition_count == 0) {
        // Called before log_manager, this is an internal kvstore, give it a
        // full falloc step.
        return guess;
    }

    // Initial disk stats read happens very early in startup, we should
    // never be called before that.
    vassert(_space_allowance > 0, "Called before disk stats init");

    if (_space_allowance > 0 && _partition_count > 0) {
        // Pessimistic assumption that each shard may use _at most_ the
        // disk space divided by the shard count.  If allocation of partitions
        // is uneven, this may lead to us underestimasting how much space
        // is available, which is safe.
        uint64_t space_free_this_shard = _space_allowance_free / ss::smp::count;

        // If we handed out some space more recently than the last background
        // update to our disk stats, assume the stats are out by that amount.
        if (space_free_this_shard < _projected_consumption) {
            space_free_this_shard = 0;
        } else {
            space_free_this_shard -= _projected_consumption;
        }

        auto adjusted_partition_count = _partition_count;
        if (_partition_count > _projected_partitions) {
            // Adjust the partition count to keep a steady falloc size
            // when a large number of partitions ask for space in a short
            // period, e.g. when creating a topic.  Otherwise, we would
            // end up giving smaller falloc steps to partitions that asked
            // later.
            // This isn't foolproof: fallocs from unrelated partitions
            // in the meantime will interfere, and calls to update_allowance()
            // in the middle of a batch of partition creations will interfere,
            // but it's safe in those paths, and gives a neater behaviour
            // in the happy path.

            // FIXME: this doesn't seem to be working as expected
            adjusted_partition_count -= _projected_partitions;
        }

        // Only use up to half the available space for fallocs.
        uint64_t space_per_partition = space_free_this_shard
                                       / (adjusted_partition_count * 2);

        guess = std::min(space_per_partition, guess);
        vlog(
          stlog.trace,
          "get_falloc_step: guess {} space per partition: {} ({}/{} {})",
          guess,
          space_per_partition,
          _space_allowance_free,
          _space_allowance,
          _partition_count);
    } else {
        vlog(
          stlog.trace,
          "get_falloc_step: not initialized yet {} {} {}",
          _space_allowance,
          _space_allowance_free,
          _partition_count);
    }

    if (segment_size_hint) {
        // Don't falloc more than the segment size, plus a little extra because
        // we don't roll segments until they overshoot the size.
        guess = std::min(
          guess, segment_size_hint.value() + _append_chunk_size())
    }

    // Round down to nearest append chunk size
    auto remainder = guess % _append_chunk_size();
    guess = guess - remainder;
    vlog(stlog.trace, "get_falloc_step: rounded to {}", guess);

    // At the minimum, falloc one chunk's worth of space.
    if (guess < min_falloc_step) {
        // If we have less than the minimum step, don't both falloc'ing at all.
        guess = _append_chunk_size();
    }

    vlog(
      stlog.trace,
      "get_falloc_step: guess {} (vs max {})",
      guess,
      _segment_fallocation_step());
    _projected_partitions += 1;
    _projected_consumption += guess;
    return guess;
}

} // namespace storage
