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

#include "config/property.h"
#include "units.h"

#include <cstdint>

namespace storage {

/**
 * This class is used by various storage components to control consumption
 * of shared system resources.  It broadly does this in two ways:
 * - Limiting concurrency of certain types of operation
 * - Controlling buffer sizes depending on available resources
 */
class storage_resources {
public:
    // If we don't have this much disk space available per partition,
    // don't both falloc'ing at all.
    static constexpr size_t min_falloc_step = 128_KiB;

    storage_resources();
    storage_resources(const storage_resources&) = delete;

    /**
     * Call this when the storage::node_api state is updated
     */
    void update_allowance(uint64_t total, uint64_t free);

    /**
     * Call this when topics_table gets updated
     */
    void update_partition_count(size_t partition_count) {
        _partition_count = partition_count;
    }

    uint64_t get_space_allowance() { return _space_allowance; }

    size_t get_falloc_step();

private:
    uint64_t _space_allowance;
    uint64_t _space_allowance_free;

    size_t _partition_count;
    config::binding<size_t> _segment_fallocation_step;
    config::binding<size_t> _append_chunk_size;

    // Keep track of how much space we've handed out in
    // between calls to update_allowance().
    uint64_t _projected_consumption;
};

} // namespace storage