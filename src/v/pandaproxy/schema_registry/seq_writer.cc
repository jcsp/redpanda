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
#include "vlog.h"

#include <seastar/core/coroutine.hh>

using namespace std::chrono_literals;

namespace pandaproxy::schema_registry {

/// Call this before reading from the store, if servicing
/// a REST API endpoint that requires global knowledge of latest
/// data (i.e. any listings)
ss::future<> seq_writer::read_sync() {
    co_await _store.sync([this]() -> ss::future<> {
        co_await read_sync_inner();
        co_return;
    });
}

ss::future<> seq_writer::read_sync_inner() {
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

    auto last_offset = co_await _store.get_loaded_offset();
    if (partition.offset - 1 > last_offset) {
        vlog(
          plog.debug,
          "read_sync dirty!  Reading {}..{}",
          last_offset,
          partition.offset);

        // TODO: generalize validation of the seq numbers of records we read
        //       (currently special cased for schema_key)

        co_await make_client_fetch_batch_reader(
          _client.local(),
          model::schema_registry_internal_tp,
          last_offset + model::offset{1},
          partition.offset)
          .consume(consume_to_store{_store}, model::no_timeout);
    } else {
        vlog(plog.debug, "read_sync clean (offset  {})", partition.offset);
    }

    co_return;
}

ss::future<std::vector<schema_version>>
seq_writer::delete_subject_impermanent(subject sub) {
    vlog(plog.debug, "delete_subject_impermanent sub={}", sub);
    auto do_write = [sub, this](model::offset write_at)
      -> ss::future<std::optional<std::vector<schema_version>>> {
        // Grab the versions before they're gone.
        std::vector<schema_version> versions = co_await _store.get_versions(
          sub, include_deleted::yes);

        // Inspect the subject to see if its already deleted
        if (co_await _store.is_subject_deleted(sub)) {
            co_return std::make_optional(versions);
        }

        // Proceed to write
        auto my_node_id = config::shard_local_cfg().node_id();
        auto version = versions.back();
        auto key = delete_subject_key{
          .seq{write_at}, .node{my_node_id}, .sub{sub}};
        auto value = delete_subject_value{.sub{sub}, .version{version}};
        auto batch = as_record_batch(key, value);

        auto success = co_await produce_and_check(write_at, std::move(batch));
        if (success) {
            auto applier = consume_to_store(_store);
            co_await applier.apply(write_at, key, value);
            co_await _store.replay(write_at);
            co_return versions;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write<std::vector<schema_version>>(
      [do_write](model::offset next_offset) { return do_write(next_offset); });
}

/// Permanent deletions (i.e. writing tombstones for previous sequenced
/// records) do not themselves need sequence numbers.
/// Include a version if we are only to hard delete that version, otherwise
/// will hard-delete the whole subject.
ss::future<std::vector<schema_version>> seq_writer::delete_subject_permanent(
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

    std::vector<std::variant<schema_key, delete_subject_key>> keys;
    for (auto s : sequences) {
        vlog(
          plog.debug,
          "Delete subject_permanent: tombstoning sub={} at {}",
          sub,
          s);

        // FIXME: assuming magic is the same as it was when key was
        // originally read... remove magic field now that we aren't aiming
        // for topic-level compatibility?
        if (s.delete_subject) {
            auto key = delete_subject_key{
              .seq{s.seq}, .node{s.node}, .sub{sub}, .magic{0}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        } else {
            auto key = schema_key{
              .seq{s.seq},
              .node{s.node},
              .sub{sub},
              .version{s.version},
              .magic{1}};
            keys.push_back(key);
            rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
        }
    }

    // If a subject is in the store, it must have been replayed some somewhere,
    // so there must be some entries in the list of keys to tombstone.
    assert(!keys.empty());

    // Produce tombstones.  We do not need to check where they landed, because
    // these can arrive in any order and be safely repeated.
    auto batch = std::move(rb).build();
    assert(batch.record_count() > 0);

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

    // Replay the persisted deletions into our store
    auto applier = consume_to_store(_store);
    auto offset = res.base_offset;
    for (auto k : keys) {
        if (auto skey = std::get_if<schema_key>(&k)) {
            co_await applier.apply(offset, *skey, std::nullopt);
        } else if (auto dkey = std::get_if<delete_subject_key>(&k)) {
            co_await applier.apply(offset, *dkey, std::nullopt);
        }
        co_await _store.replay(offset);
        offset++;
    }

    // TODO: deleting config_key entries, need to remember their sequence
    // numbers too.  Actually... do config_key entries really need
    // to be strictly ordered?  We're not allocating anything.

    co_return std::vector<schema_version>();
}

/// Helper for write methods that need to check + retry if their
/// write landed where they expected it to.
///
/// \param write_at Offset at which caller expects their write to land
/// \param batch Message to write
/// \return true if the write landed at `write_at`, else false
ss::future<bool> seq_writer::produce_and_check(
  model::offset write_at, model::record_batch batch) {
    kafka::partition_produce_response res
      = co_await _client.local().produce_record_batch(
        model::schema_registry_internal_tp, std::move(batch));

    // TODO(Ben): Check the error reporting here
    if (res.error_code != kafka::error_code::none) {
        throw kafka::exception(res.error_code, *res.error_message);
    }

    auto wrote_at = res.base_offset;
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

/// Wrapper around client writes, providing sequence number
/// logic to de-conflict concurrent writes from remote nodes.
ss::future<schema_id> seq_writer::write_subject_version(
  subject sub, schema_definition def, schema_type type) {
    auto do_write =
      [sub, def, type, this](
        model::offset write_at) -> ss::future<std::optional<schema_id>> {
        // FIXME: once we centralize the last offset info in sharded_store,
        // it will be sane to just use whatever's in cache for our most recent
        // state. But for the moment do a read_sync on each

        // Check if store already contains this data: if
        // so, we do no I/O and return the schema ID.

        // IMPORTANT: when we fail to commit our write, we must go all teh way
        // back to the beginning, including checking if the insertion already
        // exists in the store.

        // Ordering: it's important that we get our projected write
        // location before we project the IDs.  That way if something
        // else writes (and invalidates our projected IDs) in the meantime,
        // we'll just retry, rather than writing invalid stuff.
        auto projected = co_await _store.project_ids(sub, def, type);

        if (!projected.inserted) {
            vlog(plog.debug, "write_subject_version: no-op");
            co_return projected.id;
        } else {
            vlog(
              plog.debug,
              "seq_writer::write_subject_version project offset={} subject={} "
              "schema={} "
              "version={}",
              write_at,
              sub,
              projected.id,
              projected.version);

            auto my_node_id = config::shard_local_cfg().node_id();

            auto key = schema_key{
              .seq{write_at},
              .node{my_node_id},
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

            kafka::partition_produce_response res
              = co_await _client.local().produce_record_batch(
                model::schema_registry_internal_tp, std::move(batch));

            // TODO(Ben): Check the error reporting here
            if (res.error_code != kafka::error_code::none) {
                throw kafka::exception(res.error_code, *res.error_message);
            }

            auto wrote_at = res.base_offset;
            if (wrote_at == write_at) {
                vlog(
                  plog.debug,
                  "write_subject_version: write success (landed at {})",
                  wrote_at);

                // On successful write, replay the key+value we just wrote
                // to the in-memory store
                auto applier = consume_to_store(_store);
                co_await applier.apply(wrote_at, key, value);
                co_await _store.replay(wrote_at);
                co_return projected.id;
            } else {
                vlog(
                  plog.debug,
                  "write_subject_version: write fail (landed at {})",

                  wrote_at);
                co_return std::nullopt;
            }
        }
    };

    co_return co_await sequenced_write<schema_id>(
      [do_write](model::offset next_offset) { return do_write(next_offset); });
}

/// Impermanent delete: update a version with is_deleted=true
ss::future<bool>
seq_writer::delete_subject_version(subject sub, schema_version version) {
    auto do_write =
      [sub, version, this](
        model::offset write_at) -> ss::future<std::optional<bool>> {
        auto s_res = co_await _store.get_subject_schema(
          sub, version, include_deleted::yes);
        subject_schema ss = std::move(s_res);

        auto my_node_id = config::shard_local_cfg().node_id();
        auto key = schema_key{
          .seq{write_at}, .node{my_node_id}, .sub{sub}, .version{version}};
        vlog(plog.debug, "seq_writer::delete_subject_version {}", key);
        auto value = schema_value{
          .sub{sub},
          .version{version},
          .type = ss.type,
          .id{ss.id},
          .schema{std::move(ss.definition)},
          .deleted{is_deleted::yes}};

        auto batch = as_record_batch(key, value);

        auto success = co_await produce_and_check(write_at, std::move(batch));
        if (success) {
            auto applier = consume_to_store(_store);
            co_await applier.apply(write_at, key, value);
            co_await _store.replay(write_at);
            co_return true;
        } else {
            // Pass up a None, our caller's cue to retry
            co_return std::nullopt;
        }
    };

    co_return co_await sequenced_write<bool>(
      [do_write](model::offset next_offset) { return do_write(next_offset); });
}

ss::future<> seq_writer::back_off() {
    // TODO: add jitter
    vlog(plog.debug, "Write collision, backing off");
    co_await ss::sleep(10ms);
    // Make sure we've seen latest offset before trying again
    co_await read_sync();
}

} // namespace pandaproxy::schema_registry
