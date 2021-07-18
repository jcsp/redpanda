/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/fundamental.h"
#include "pandaproxy/schema_registry/types.h"

#include <seastar/core/sharded.hh>

namespace pandaproxy::schema_registry {

class store;

///\brief Dispatch requests to shards based on a a hash of the
/// subject or schema_id
class sharded_store {
public:
    ss::future<> start(ss::smp_service_group sg);
    ss::future<> stop();

    struct insert_result {
        schema_version version;
        schema_id id;
        bool inserted;
    };

    ss::future<insert_result>
    project_ids(subject sub, schema_definition def, schema_type type);

    ss::future<bool> upsert(
      subject sub,
      schema_definition def,
      schema_type type,
      schema_id id,
      schema_version version,
      is_deleted deleted);

    ///\brief Return a schema by id.
    ss::future<schema> get_schema(const schema_id& id);

    ///\brief Return a schema by subject and version.
    ss::future<subject_schema> get_subject_schema(
      const subject& sub, schema_version version, include_deleted inc_dec);

    ///\brief Return a list of subjects.
    ss::future<std::vector<subject>> get_subjects(include_deleted inc_del);

    ///\brief Return a list of versions and associated schema_id.
    ss::future<std::vector<schema_version>>
    get_versions(const subject& sub, include_deleted inc_del);

    ///\brief Delete a subject.
    ss::future<std::vector<schema_version>>
    delete_subject(const subject& sub, permanent_delete permanent);

    ///\brief Delete a subject version
    ss::future<bool> delete_subject_version(
      const subject& sub,
      schema_version version,
      permanent_delete permanent,
      include_deleted inc_del);

    ///\brief Get the global compatibility level.
    ss::future<compatibility_level> get_compatibility();

    ///\brief Get the compatibility level for a subject, or fallback to global.
    ss::future<compatibility_level> get_compatibility(const subject& sub);

    ///\brief Set the global compatibility level.
    ss::future<bool> set_compatibility(compatibility_level compatibility);

    ///\brief Set the compatibility level for a subject.
    ss::future<bool>
    set_compatibility(const subject& sub, compatibility_level compatibility);

    ///\brief Clear the compatibility level for a subject.
    ss::future<bool> clear_compatibility(const subject& sub);

    ///\brief Check if the provided schema is compatible with the subject and
    /// version, according the the current compatibility.
    ///
    /// If the compatibility level is transitive, then all versions are checked,
    /// otherwise checks are against the version provided and newer.
    ss::future<bool> is_compatible(
      const subject& sub,
      schema_version version,
      const schema_definition& new_schema,
      schema_type new_schema_type);

    ss::future<model::offset> get_loaded_offset();
    ss::future<> replay(model::offset off);

    /// The _sync_sem does not protect any member fields, it is just to
    /// stop concurrent GET operations from generating spurious concurrent
    /// reads to the topic.
    template<typename F>
    ss::future<> sync(F f) {
        auto _guard = co_await ss::get_units(_sync_sem, 1);
        co_await f();
    }

    ss::future<model::offset> project_write();
    ss::future<> complete_write();

private:
    struct insert_schema_result {
        schema_id id;
        bool inserted;
    };

    ss::future<bool>
    upsert_schema(schema_id id, schema_definition def, schema_type type);

    struct insert_subject_result {
        schema_version version;
        bool inserted;
    };
    ss::future<insert_subject_result> insert_subject(subject sub, schema_id id);

    ss::future<bool> upsert_subject(
      subject sub, schema_version version, schema_id id, is_deleted deleted);

    ss::future<> maybe_update_max_schema_id(schema_id id);

    ss::future<schema_id> project_schema_id();

    ss::smp_submit_to_options _smp_opts;
    ss::sharded<store> _store;

    ///\brief Access must occur only on shard 0.
    schema_id _next_schema_id{1};

    /// Shard 0 only: Reads have progressed as far as this offset
    model::offset _loaded_offset{-1};

    /// Shard 0 only: Writes are in flight up to this offset
    model::offset _projected_write_offset{-1};

    /// Shard 0 only: Serialize read_sync operations, to avoid issuing
    /// gratuitous number of reads to the topic on concurrent GETs.
    ss::semaphore _sync_sem{1};

    /// Shard 0 only: Serialize write and replay operations.  May be
    /// held inside _sync_sem
    ss::semaphore _seq_sem{1};
};

} // namespace pandaproxy::schema_registry
