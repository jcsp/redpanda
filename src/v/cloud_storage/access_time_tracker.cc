/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/access_time_tracker.h"

#include "serde/serde.h"
#include "units.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/smp.hh>

#include <absl/container/btree_map.h>

#include <exception>
#include <variant>

namespace absl {

template<class Key, class Value>
void read_nested(
  iobuf_parser& in,
  btree_map<Key, Value>& btree,
  size_t const bytes_left_limit) {
    using serde::read_nested;
    uint64_t sz;
    read_nested(in, sz, bytes_left_limit);
    for (auto i = 0UL; i < sz; i++) {
        Key key;
        Value value;
        read_nested(in, key, bytes_left_limit);
        read_nested(in, value, bytes_left_limit);
        btree.insert({key, value});
    }
}

} // namespace absl

namespace cloud_storage {

// For async serialization/deserialization, the fixed-size content
// is defined in a header struct, and then the main body of the encoding
// is defined by hand in the read/write methods so that it can be done
// with streaming.
struct table_header
  : serde::envelope<table_header, serde::version<0>, serde::compat_version<0>> {
    size_t table_size{0};

    auto serde_fields() { return std::tie(table_size); }
};

ss::future<> access_time_tracker::write(ss::output_stream<char>& out) {
    auto lock_guard = co_await ss::get_units(_serde_lock, 1);
    _dirty = false;

    const table_header h{.table_size = _table.size()};
    iobuf header_buf;
    serde::write(header_buf, h);

    for (const auto& f : header_buf) {
        co_await out.write(f.get(), f.size());
    }

    // How many items to serialize per stream write()
    constexpr size_t chunk_count = 2048;

    size_t i = 0;
    iobuf serialize_buf;
    for (auto it : _table) {
        serde::write(serialize_buf, it.first);
        serde::write(serialize_buf, it.second);
        ++i;
        if (i % chunk_count == 0 || i == _table.size()) {
            for (const auto& f : serialize_buf) {
                co_await out.write(f.get(), f.size());
            }
            serialize_buf.clear();
        }
    }
}

ss::future<> access_time_tracker::read(ss::input_stream<char>& in) {
    auto lock_guard = co_await ss::get_units(_serde_lock, 1);
    _table.clear();
    _dirty = false;

    // Accumulate a serialized table_header in this buffer
    iobuf header_buf;

    // Read serde envelope header
    auto envelope_header_tmp = co_await in.read_exactly(
      serde::envelope_header_size);
    header_buf.append(envelope_header_tmp.get(), envelope_header_tmp.size());

    // Peek at the size of the header's serde body
    iobuf envelope_header_buf = header_buf.copy();
    auto peek_parser = iobuf_parser(std::move(envelope_header_buf));
    auto header_size = serde::peek_body_size(peek_parser);

    // Read the rest of the header + decode it
    auto tmp = co_await in.read_exactly(header_size);
    header_buf.append(tmp.get(), tmp.size());
    auto h_parser = iobuf_parser(std::move(header_buf));
    table_header h = serde::read_nested<table_header>(h_parser, 0);

    // How many items to consume per stream read()
    constexpr size_t chunk_count = 2048;

    for (size_t i = 0; i < h.table_size; i += chunk_count) {
        auto item_count = std::min(chunk_count, h.table_size - i);
        auto tmp_buf = co_await in.read_exactly(item_count * table_item_size);
        iobuf items_buf;
        items_buf.append(std::move(tmp_buf));
        auto parser = iobuf_parser(std::move(items_buf));
        for (size_t j = 0; j < item_count; ++j) {
            uint32_t hash = serde::read_nested<uint32_t>(parser, 0);
            timestamp_t t = serde::read_nested<timestamp_t>(parser, 0);
            _table.emplace(hash, t);
        }
    }
}

ss::future<> access_time_tracker::add_timestamp(
  std::string_view key, std::chrono::system_clock::time_point ts) {
    co_await table_write_barrier();

    uint32_t seconds = std::chrono::time_point_cast<std::chrono::seconds>(ts)
                         .time_since_epoch()
                         .count();
    uint32_t hash = xxhash_32(key.data(), key.size());

    _table[hash] = seconds;
    _dirty = true;
}

ss::future<>
access_time_tracker::remove_timestamp(std::string_view key) noexcept {
    co_await table_write_barrier();

    try {
        uint32_t hash = xxhash_32(key.data(), key.size());
        _table.erase(hash);
        _dirty = true;
    } catch (...) {
        vassert(
          false,
          "Can't remove key {} from access_time_tracker, exception: {}",
          key,
          std::current_exception());
    }
}

ss::future<>
access_time_tracker::trim(const fragmented_vector<file_list_item>& existent) {
    absl::btree_set<uint32_t> existent_hashes;
    for (const auto& i : existent) {
        existent_hashes.insert(xxhash_32(i.path.data(), i.path.size()));
    }

    table_t tmp;
    for (auto it : _table) {
        if (existent_hashes.contains(it.first)) {
            tmp.insert(it);
        }
        co_await ss::maybe_yield();
    }
    _table = std::move(tmp);
}

std::optional<std::chrono::system_clock::time_point>
access_time_tracker::estimate_timestamp(std::string_view key) const {
    uint32_t hash = xxhash_32(key.data(), key.size());
    auto it = _table.find(hash);
    if (it == _table.end()) {
        return std::nullopt;
    }
    auto seconds = std::chrono::seconds(it->second);
    std::chrono::system_clock::time_point ts(seconds);
    return ts;
}

bool access_time_tracker::is_dirty() const { return _dirty; }

ss::future<> access_time_tracker::table_write_barrier() {
    if (unlikely(_serde_lock.current() == 0)) {
        co_await get_units(_serde_lock, 1);
    }
}

} // namespace cloud_storage
