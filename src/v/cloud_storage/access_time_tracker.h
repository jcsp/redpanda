/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "hashing/xx.h"
#include "recursive_directory_walker.h"
#include "seastar/core/iostream.hh"
#include "seastarx.h"
#include "serde/envelope.h"
#include "utils/mutex.h"

#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>

#include <chrono>
#include <string_view>

namespace cloud_storage {

/// Access time tracker maintains map from filename hash to
/// the timestamp that represents the time when the file was
/// accessed last.
///
/// It is possible to have conflicts. In case of conflict
/// 'add_timestamp' method will overwrite another key. For that
/// key we will observe larger access time. When one of the
/// conflicted entries will be deleted another will be deleted
/// as well. This is OK because the code in the
/// 'cloud_storage/cache_service' is ready for that.
class access_time_tracker {
    using timestamp_t = uint32_t;
    using table_t = absl::btree_map<uint32_t, timestamp_t>;

    // Serialized size of each pair in table_t
    static constexpr size_t table_item_size = 8;

public:
    /// Add access time to the container.
    ss::future<> add_timestamp(
      std::string_view key, std::chrono::system_clock::time_point ts);

    /// Remove key from the container.
    ss::future<> remove_timestamp(std::string_view) noexcept;

    /// Return access time estimate (it can differ if there is a conflict
    /// on file name hash).
    std::optional<std::chrono::system_clock::time_point>
    estimate_timestamp(std::string_view key) const;

    ss::future<> write(ss::output_stream<char>&);
    ss::future<> read(ss::input_stream<char>&);

    /// Returns true if tracker has new data which wasn't serialized
    /// to disk.
    bool is_dirty() const;

    /// Remove every key which isn't present in list of existing files
    ss::future<> trim(const std::vector<file_list_item>&);

    size_t size() const { return _table.size(); }

private:
    // Lock taken during async serialization/deserialization.  Synchronous
    // modifications may proceed without the lock if it is not taken.
    ss::semaphore _serde_lock{1};

    // Call this before making synchronous changes to _table.  Ensures
    // that _serde_lock is not held before proceeding.
    ss::future<> table_write_barrier();

    absl::btree_map<uint32_t, timestamp_t> _table;
    bool _dirty{false};
};

} // namespace cloud_storage
