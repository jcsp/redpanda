//// Copyright 2021 Vectorized, Inc.
////
//// Use of this software is governed by the Business Source License
//// included in the file licenses/BSL.md
////
//// As of the Change Date specified in that file, in accordance with
//// the Business Source License, use of this software will be governed
//// by the Apache License, Version 2.

#include "pandaproxy/schema_registry/seq_writer.h"

#include "pandaproxy/error.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/client_fetch_batch_reader.h"
#include "pandaproxy/schema_registry/storage.h"
#include "random/simple_time_jitter.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace pandaproxy::schema_registry {

/// Call this before reading from the store, if servicing
/// a REST API endpoint that requires global knowledge of latest
/// data (i.e. any listings)
///
/// Multiple callers will not block each other if our local store
/// is up to date.  If something needs loading, only one caller at
/// a time will do the load.
/// \return
ss::future<> seq_writer::read_sync() {
    auto offsets = co_await _client.local().list_offsets(
      model::schema_registry_internal_tp);

    const auto& topics = offsets.data.topics;
    if (topics.size() != 1 || topics.front().partitions.size() != 1) {
        auto ec = kafka::error_code::unknown_topic_or_partition;
        throw kafka::exception(ec, make_error_code(ec).message());
    }

    const auto& partition = topics.front().partitions.front();
    if (partition.error_code != kafka::error_code::none) {
        auto ec = partition.error_code;
        throw kafka::exception(ec, make_error_code(ec).message());
    }

    co_await wait(partition.offset - model::offset{1});
}

/// Helper for write methods that need to check + retry if their
/// write landed where they expected it to.
///
/// \param write_at Offset at which caller expects their write to land
/// \param batch Message to write
/// \return true if the write landed at `write_at`, else false
ss::future<bool> seq_writer::produce_and_check(
  model::offset write_at, model::record_batch batch) {
    // Because we rely on checking exactly where our message (singular) landed,
    // only use this function with batches of a single message.
    vassert(batch.record_count() == 1, "Only single-message batches allowed");

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));

    // TODO(Ben): Check the error reporting here
    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(res.error_code, *res.error_message);
    }

    auto wrote_at = res.base_offset;

    // If we succeed, wait to make sure results are readable
    // If we fail, wait to make sure we have an up to date offset before retry
    co_await wait(wrote_at);

    if (wrote_at == write_at) {
        vlog(plog.debug, "seq_writer: Successful write at {}", wrote_at);

        co_return true;
    } else {
        vlog(
          plog.debug,
          "seq_writer: Failed write at {} (wrote at {})",
          write_at,
          wrote_at);
        co_return false;
    }
};

ss::future<> seq_writer::advance_offset(model::offset offset) {
    auto remote = [offset](seq_writer& s) { s.advance_offset_inner(offset); };

    return container().invoke_on(0, _smp_opts, remote);
}

void seq_writer::advance_offset_inner(model::offset offset) {
    vlog(
      plog.debug,
      "seq_writer::advance_offset {}->{}",
      _waiters.get_offset(),
      offset);
    _waiters.signal(offset);
}

ss::future<> seq_writer::wait(model::offset offset) {
    auto do_wait = [this, offset] { return _waiters.wait(offset); };
    return ss::smp::submit_to(ss::shard_id{0}, _smp_opts, std::move(do_wait));
}

ss::future<schema_id> seq_writer::write_subject_version(
  subject sub, schema_definition def, schema_type type) {
    auto do_write = [sub, def, type](
                      model::offset write_at,
                      seq_writer& seq) -> ss::future<std::optional<schema_id>> {
        // Check if store already contains this data: if
        // so, we do no I/O and return the schema ID.
        auto projected = co_await seq._store.project_ids(sub, def, type);

        if (!projected.inserted) {
            vlog(plog.debug, "write_subject_version: no-op");
            co_return projected.id;
        } else {
            vlog(
              plog.debug,
              "seq_writer::write_subject_version project offset={} "
              "subject={} "
              "schema={} "
              "version={}",
              write_at,
              sub,
              projected.id,
              projected.version);

            auto key = schema_key{
              .seq{write_at},
              .node{seq._node_id},
              .sub{sub},
              .version{projected.version}};
            auto value = schema_value{
              .sub{std::move(sub)},
              .version{projected.version},
              .type = type,
              .id{projected.id},
              .schema{std::move(def)},
              .deleted = is_deleted::no};

            auto batch = as_record_batch(key, value);

            auto success = co_await seq.produce_and_check(
              write_at, std::move(batch));
            if (success) {
                co_return projected.id;
            } else {
                co_return std::nullopt;
            }
        }
    };

    return sequenced_write(do_write);
}

ss::future<bool> seq_writer::write_config(
  std::optional<subject> sub, compatibility_level compat) {
    auto do_write = [sub, compat](
                      model::offset write_at,
                      seq_writer& seq) -> ss::future<std::optional<bool>> {
        vlog(
          plog.debug,
          "write_config sub={} compat={} offset={}",
          sub,
          to_string_view(compat),
          write_at);

        // Check for no-op case
        compatibility_level existing;
        if (sub.has_value()) {
            existing = co_await seq._store.get_compatibility(sub.value());
        } else {
            existing = co_await seq._store.get_compatibility();
        }
        if (existing == compat) {
            co_return false;
        }

        auto key = config_key{
          .seq{write_at}, .node{seq._node_id}, .sub{std::move(sub)}};
        auto value = config_value{.compat = compat};
        auto batch = as_record_batch(key, value);

        auto success = co_await seq.produce_and_check(
          write_at, std::move(batch));
        if (success) {
            co_return true;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write(do_write);
}

/// Impermanent delete: update a version with is_deleted=true
ss::future<bool>
seq_writer::delete_subject_version(subject sub, schema_version version) {
    auto do_write = [sub, version](
                      model::offset write_at,
                      seq_writer& seq) -> ss::future<std::optional<bool>> {
        auto s_res = co_await seq._store.get_subject_schema(
          sub, version, include_deleted::yes);
        subject_schema ss = std::move(s_res);

        auto key = schema_key{
          .seq{write_at}, .node{seq._node_id}, .sub{sub}, .version{version}};
        vlog(plog.debug, "seq_writer::delete_subject_version {}", key);
        auto value = schema_value{
          .sub{sub},
          .version{version},
          .type = ss.type,
          .id{ss.id},
          .schema{std::move(ss.definition)},
          .deleted{is_deleted::yes}};

        auto batch = as_record_batch(key, value);

        auto success = co_await seq.produce_and_check(
          write_at, std::move(batch));
        if (success) {
            co_return true;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write(do_write);
}

ss::future<std::vector<schema_version>>
seq_writer::delete_subject_impermanent(subject sub) {
    vlog(plog.debug, "delete_subject_impermanent sub={}", sub);
    auto do_write = [sub](model::offset write_at, seq_writer& seq)
      -> ss::future<std::optional<std::vector<schema_version>>> {
        // Grab the versions before they're gone.
        std::vector<schema_version> versions = co_await seq._store.get_versions(
          sub, include_deleted::yes);

        // Inspect the subject to see if its already deleted
        if (co_await seq._store.is_subject_deleted(sub)) {
            co_return std::make_optional(versions);
        }

        // Proceed to write
        auto version = versions.back();
        auto key = delete_subject_key{
          .seq{write_at}, .node{seq._node_id}, .sub{sub}};
        auto value = delete_subject_value{.sub{sub}, .version{version}};
        auto batch = as_record_batch(key, value);

        auto success = co_await seq.produce_and_check(
          write_at, std::move(batch));
        if (success) {
            co_return versions;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write(do_write);
}

/// Permanent deletions (i.e. writing tombstones for previous sequenced
/// records) do not themselves need sequence numbers.
/// Include a version if we are only to hard delete that version, otherwise
/// will hard-delete the whole subject.
ss::future<std::vector<schema_version>> seq_writer::delete_subject_permanent(
  subject sub, std::optional<schema_version> version) {
    return container().invoke_on(0, _smp_opts, [sub, version](seq_writer& seq) {
        return ss::with_semaphore(seq._write_sem, 1, [sub, version, &seq]() {
            return seq.delete_subject_permanent_inner(sub, version);
        });
    });
}

ss::future<std::vector<schema_version>>
seq_writer::delete_subject_permanent_inner(
  subject sub, std::optional<schema_version> version) {
    std::vector<seq_marker> sequences;
    /// Check for whether our victim is already soft-deleted happens
    /// within these store functions (will throw a 404-equivalent if so)
    vlog(plog.debug, "delete_subject_permanent sub={}", sub);
    if (version.has_value()) {
        sequences = co_await _store.get_subject_version_written_at(
          sub, version.value());
    } else {
        sequences = co_await _store.get_subject_written_at(sub);
    }

    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    std::vector<std::variant<schema_key, delete_subject_key, config_key>> keys;
    for (auto s : sequences) {
        vlog(
          plog.debug,
          "Delete subject_permanent: tombstoning sub={} at {}",
          sub,
          s);

        // Assumption: magic is the same as it was when key was
        // originally read.
        switch (s.key_type) {
        case seq_marker_key_type::schema: {
            auto key = schema_key{
              .seq{s.seq}, .node{s.node}, .sub{sub}, .version{s.version}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::delete_subject: {
            auto key = delete_subject_key{
              .seq{s.seq}, .node{s.node}, .sub{sub}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        case seq_marker_key_type::config: {
            auto key = config_key{.seq{s.seq}, .node{s.node}, .sub{sub}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } break;
        default:
            vassert(false, "Unknown key type");
        }
    }

    // Produce tombstones.  We do not need to check where they landed,
    // because these can arrive in any order and be safely repeated.
    auto batch = std::move(rb).build();

    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));
    if (res.error_code != kafka::error_code::none) {
        vlog(
          plog.error,
          "Error writing to schema topic: {} {}",
          res.error_code,
          res.error_message);
        throw kafka::exception(res.error_code, *res.error_message);
    }

    // Wait for deletions to replay into our store
    co_await wait(res.base_offset + model::offset{keys.size()});
    co_return std::vector<schema_version>();
}

} // namespace pandaproxy::schema_registry
