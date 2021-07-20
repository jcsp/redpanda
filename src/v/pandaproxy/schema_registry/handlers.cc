// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "handlers.h"

#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/parsing/httpd.h"
#include "pandaproxy/reply.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/requests/compatibility.h"
#include "pandaproxy/schema_registry/requests/config.h"
#include "pandaproxy/schema_registry/requests/get_schemas_ids_id.h"
#include "pandaproxy/schema_registry/requests/get_subject_versions_version.h"
#include "pandaproxy/schema_registry/requests/post_subject_versions.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/server.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/std-coroutine.hh>

namespace ppj = pandaproxy::json;

namespace pandaproxy::schema_registry {

using server = ctx_server<service>;

void parse_accept_header(const server::request_t& rq, server::reply_t& rp) {
    static const std::vector<ppj::serialization_format> headers{
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_json,
      ppj::serialization_format::application_json,
      ppj::serialization_format::none};
    rp.mime_type = parse::accept_header(*rq.req, headers);
}

void parse_content_type_header(const server::request_t& rq) {
    static const std::vector<ppj::serialization_format> headers{
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_json,
      ppj::serialization_format::application_json,
      ppj::serialization_format::application_octet};
    parse::content_type_header(*rq.req, headers);
}

ss::future<server::reply_t>
get_config(server::request_t rq, server::reply_t rp) {
    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    parse_accept_header(rq, rp);
    rq.req.reset();

    auto res = co_await rq.service().schema_store().get_compatibility();

    auto json_rslt = ppj::rjson_serialize(get_config_req_rep{.compat = res});
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
put_config(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto config = ppj::rjson_parse(
      rq.req->content.data(), put_config_handler<>{});
    rq.req.reset();

    auto res = co_await rq.service().schema_store().set_compatibility(
      config.compat);

    if (res) {
        auto res = co_await rq.service().client().local().produce_record_batch(
          model::schema_registry_internal_tp,
          make_config_batch(std::nullopt, config.compat));

        // TODO(Ben): Check the error reporting here
        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(res.error_code, *res.error_message);
        }
    }

    auto json_rslt = ppj::rjson_serialize(config);
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
get_config_subject(server::request_t rq, server::reply_t rp) {
    // Ensure we see latest writes
    co_await rq.service().writer().read_sync();

    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    rq.req.reset();

    auto res = co_await rq.service().schema_store().get_compatibility(sub);

    auto json_rslt = ppj::rjson_serialize(get_config_req_rep{.compat = res});
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
put_config_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto config = ppj::rjson_parse(
      rq.req->content.data(), put_config_handler<>{});
    rq.req.reset();

    auto res = co_await rq.service().schema_store().set_compatibility(
      sub, config.compat);

    if (res) {
        auto res = co_await rq.service().client().local().produce_record_batch(
          model::schema_registry_internal_tp,
          make_config_batch(sub, config.compat));

        // TODO(Ben): Check the error reporting here
        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(res.error_code, *res.error_message);
        }
    }

    auto json_rslt = ppj::rjson_serialize(config);
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
get_schemas_types(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    rq.req.reset();

    static const std::vector<std::string_view> schemas_types{"AVRO"};
    auto json_rslt = ppj::rjson_serialize(schemas_types);
    rp.rep->write_body("json", json_rslt);
    return ss::make_ready_future<server::reply_t>(std::move(rp));
}

/// For GETs that load a specific version, we usually find it in memory,
/// but if it's missing, trigger a re-read of the topic before responding
/// definitively as to whether it is present or not.
template<typename V, typename F>
auto get_or_load(server::request_t& rq, F f) -> ss::future<V> {
    try {
        co_return co_await f();
    } catch (pandaproxy::schema_registry::exception& ex) {
        if (
          ex.code() == error_code::schema_id_not_found
          || ex.code() == error_code::subject_not_found
          || ex.code() == error_code::subject_version_not_found) {
            // A missing object, we will proceed to reload to see if we can
            // find it.

        } else {
            // Not a missing object, something else went wrong
            throw;
        }
    }

    // Load latest writes and retry
    vlog(plog.debug, "get_or_load: refreshing schema store on missing item");
    co_await rq.service().writer().read_sync();
    co_return co_await f();
}

ss::future<server::reply_t>
get_schemas_ids_id(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto id = parse::request_param<schema_id>(*rq.req, "id");
    rq.req.reset();

    auto schema_val = co_await get_or_load<schema>(
      rq, [&rq, id]() -> ss::future<schema> {
          co_return co_await rq.service().schema_store().get_schema(id);
      });

    auto json_rslt = ppj::rjson_serialize(get_schemas_ids_id_response{
      .definition{std::move(schema_val.definition)}});
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
get_subjects(server::request_t rq, server::reply_t rp) {
    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    parse_accept_header(rq, rp);
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    rq.req.reset();

    auto subjects = co_await rq.service().schema_store().get_subjects(inc_del);
    auto json_rslt{json::rjson_serialize(subjects)};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
get_subject_versions(server::request_t rq, server::reply_t rp) {
    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    rq.req.reset();

    auto versions = co_await rq.service().schema_store().get_versions(
      sub, inc_del);

    auto json_rslt{json::rjson_serialize(versions)};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
post_subject_versions(server::request_t rq, server::reply_t rp) {
    // List-type request: must ensure we see latest writes
    co_await rq.service().writer().read_sync();

    parse_content_type_header(rq);
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    vlog(plog.debug, "post_subject_versions subject='{}'", sub);
    auto req = post_subject_versions_request{
      .sub = sub,
      .payload = ppj::rjson_parse(
        rq.req->content.data(), post_subject_versions_request_handler<>{})};
    rq.req.reset();

    auto schema_id = co_await rq.service().writer().write_subject_version(
      req.sub, req.payload.schema, req.payload.type);

    auto json_rslt{
      json::rjson_serialize(post_subject_versions_response{.id{schema_id}})};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<ctx_server<service>::reply_t> get_subject_versions_version(
  ctx_server<service>::request_t rq, ctx_server<service>::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub = parse::request_param<subject>(*rq.req, "subject");
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto inc_del{
      parse::query_param<std::optional<include_deleted>>(*rq.req, "deleted")
        .value_or(include_deleted::no)};
    rq.req.reset();

    auto version = invalid_schema_version;
    if (ver == "latest") {
        // We must sync to reliably say what is 'latest'
        co_await rq.service().writer().read_sync();

        auto versions = co_await rq.service().schema_store().get_versions(
          sub, inc_del);
        if (versions.empty()) {
            throw as_exception(not_found(sub, version));
        }
        version = versions.back();
    } else {
        version = parse::from_chars<schema_version>{}(ver).value();
    }

    auto get_res = co_await get_or_load<subject_schema>(
      rq, [&rq, sub, version, inc_del]() -> ss::future<subject_schema> {
          co_return co_await rq.service().schema_store().get_subject_schema(
            sub, version, inc_del);
      });

    auto json_rslt{json::rjson_serialize(post_subject_versions_version_response{
      .sub = sub,
      .id = get_res.id,
      .version = version,
      .definition = std::move(get_res.definition)})};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
delete_subject(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub{parse::request_param<subject>(*rq.req, "subject")};
    auto permanent{
      parse::query_param<std::optional<permanent_delete>>(*rq.req, "permanent")
        .value_or(permanent_delete::no)};
    rq.req.reset();

    auto versions
      = permanent
          ? co_await rq.service().writer().delete_subject_permanent(sub)
          : co_await rq.service().writer().delete_subject_impermanent(sub);

    auto json_rslt{json::rjson_serialize(versions)};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
delete_subject_version(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto sub{parse::request_param<subject>(*rq.req, "subject")};
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto permanent{
      parse::query_param<std::optional<permanent_delete>>(*rq.req, "permanent")
        .value_or(permanent_delete::no)};
    rq.req.reset();

    auto version = invalid_schema_version;
    if (ver == "latest") {
        // Must see latest data to know latest version
        co_await rq.service().writer().read_sync();

        auto versions = co_await rq.service().schema_store().get_versions(
          sub, include_deleted::yes);
        if (versions.empty()) {
            throw as_exception(not_found(sub, version));
        }
        version = versions.back();
    } else {
        version = parse::from_chars<schema_version>{}(ver).value();
    }

    auto d_res = co_await rq.service().schema_store().delete_subject_version(
      sub, version, permanent, include_deleted::no);

    if (d_res) {
        std::optional<model::record_batch> batch;
        if (permanent) {
            batch.emplace(
              make_delete_subject_version_permanently_batch(sub, version));
        } else {
            auto s_res
              = co_await rq.service().schema_store().get_subject_schema(
                sub, version, include_deleted::yes);
            batch.emplace(make_delete_subject_version_batch(std::move(s_res)));
        }
        auto res = co_await rq.service().client().local().produce_record_batch(
          model::schema_registry_internal_tp, std::move(batch).value());

        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(res.error_code, *res.error_message);
        }
    }

    auto json_rslt{json::rjson_serialize(version)};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

ss::future<server::reply_t>
compatibility_subject_version(server::request_t rq, server::reply_t rp) {
    parse_accept_header(rq, rp);
    auto ver = parse::request_param<ss::sstring>(*rq.req, "version");
    auto req = post_subject_versions_request{
      .sub = parse::request_param<subject>(*rq.req, "subject"),
      .payload = ppj::rjson_parse(
        rq.req->content.data(), post_subject_versions_request_handler<>{})};
    rq.req.reset();

    vlog(
      plog.info,
      "compatibility_subject_version: subject: {}, version: {}",
      req.sub,
      ver);
    auto version = invalid_schema_version;
    if (ver == "latest") {
        auto versions = co_await rq.service().schema_store().get_versions(
          req.sub, include_deleted::no);
        if (versions.empty()) {
            throw as_exception(not_found(req.sub, version));
        }
        version = versions.back();
    } else {
        version = parse::from_chars<schema_version>{}(ver).value();
    }

    auto get_res = co_await rq.service().schema_store().is_compatible(
      req.sub, version, req.payload.schema, req.payload.type);

    auto json_rslt{
      json::rjson_serialize(post_compatibility_res{.is_compat = get_res})};
    rp.rep->write_body("json", json_rslt);
    co_return rp;
}

} // namespace pandaproxy::schema_registry
