/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "redpanda/admin/services/internal/metastore.h"

#include "cloud_topics/level_one/domain/domain_manager.h"
#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"
#include "cluster/shard_table.h"
#include "redpanda/admin/services/utils.h"
#include "serde/protobuf/rpc.h"

#include <seastar/core/coroutine.hh>

namespace admin {

namespace {

[[noreturn]] void check_errc(cloud_topics::l1::metastore::errc ec) {
    switch (ec) {
    case cloud_topics::l1::metastore::errc::missing_ntp:
        throw serde::pb::rpc::not_found_exception("missing ntp");
    case cloud_topics::l1::metastore::errc::invalid_request:
        throw serde::pb::rpc::invalid_argument_exception();
    case cloud_topics::l1::metastore::errc::out_of_range:
        throw serde::pb::rpc::out_of_range_exception();
    case cloud_topics::l1::metastore::errc::transport_error:
        throw serde::pb::rpc::unavailable_exception("transport error");
    }
    throw serde::pb::rpc::unknown_exception();
}

} // namespace

seastar::future<proto::admin::metastore::get_offsets_response>
metastore_service_impl::get_offsets(
  serde::pb::rpc::context, proto::admin::metastore::get_offsets_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_offsets_response response;
    auto result = co_await _metastore->local().get_offsets(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    proto::admin::metastore::offsets offsets;
    offsets.set_start_offset(result.value().start_offset());
    offsets.set_next_offset(result.value().next_offset());
    response.set_offsets(std::move(offsets));
    co_return response;
}

seastar::future<proto::admin::metastore::get_size_response>
metastore_service_impl::get_size(
  serde::pb::rpc::context, proto::admin::metastore::get_size_request req) {
    const auto& topic_metadata = _topic_table->local().get_topic_metadata_ref(
      model::topic_namespace{
        model::kafka_namespace, model::topic{req.get_partition().get_topic()}});
    if (!topic_metadata) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    auto topic_id = topic_metadata->get().get_configuration().tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    proto::admin::metastore::get_size_response response;
    auto result = co_await _metastore->local().get_size(
      {*topic_id, model::partition_id{req.get_partition().get_partition()}});
    if (!result) {
        check_errc(result.error());
    }
    response.set_size_bytes(result.value().size);
    co_return response;
}

seastar::future<proto::admin::metastore::get_database_stats_response>
metastore_service_impl::get_database_stats(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::get_database_stats_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    // If we're not leader, reroute.
    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .get_database_stats(std::move(ctx), std::move(req));
    }

    // We're the leader; process locally.
    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }
    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<std::expected<
          cloud_topics::l1::database_stats,
          cloud_topics::l1::rpc::errc>> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->get_database_stats();
      });
    if (!result_exp.has_value()) {
        switch (result_exp.error()) {
        case cloud_topics::l1::rpc::errc::not_leader:
            throw serde::pb::rpc::unavailable_exception("not leader");
        case cloud_topics::l1::rpc::errc::missing_ntp:
            throw serde::pb::rpc::not_found_exception("missing ntp");
        case cloud_topics::l1::rpc::errc::timed_out:
            throw serde::pb::rpc::deadline_exceeded_exception();
        default:
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("error: {}", result_exp.error()));
        }
    }

    auto& result = result_exp.value();
    proto::admin::metastore::get_database_stats_response response;
    response.set_active_memtable_bytes(result.active_memtable_bytes);
    response.set_immutable_memtable_bytes(result.immutable_memtable_bytes);
    response.set_total_size_bytes(result.total_size_bytes);
    for (const auto& level : result.levels) {
        proto::admin::metastore::lsm_level level_proto;
        level_proto.set_level_number(level.level_number);

        for (const auto& file : level.files) {
            proto::admin::metastore::lsm_file file_proto;
            file_proto.set_epoch(file.epoch);
            file_proto.set_id(file.id);
            file_proto.set_size_bytes(file.size_bytes);
            file_proto.set_smallest_key_info(file.smallest_key_info);
            file_proto.set_largest_key_info(file.largest_key_info);
            level_proto.get_files().push_back(std::move(file_proto));
        }

        response.get_levels().push_back(std::move(level_proto));
    }

    co_return response;
}

namespace {

[[noreturn]] void throw_rpc_errc(cloud_topics::l1::rpc::errc ec) {
    switch (ec) {
    case cloud_topics::l1::rpc::errc::not_leader:
        throw serde::pb::rpc::unavailable_exception("not leader");
    case cloud_topics::l1::rpc::errc::missing_ntp:
        throw serde::pb::rpc::not_found_exception("missing ntp");
    case cloud_topics::l1::rpc::errc::timed_out:
        throw serde::pb::rpc::deadline_exceeded_exception();
    default:
        throw serde::pb::rpc::unavailable_exception(
          fmt::format("error: {}", ec));
    }
}

chunked_vector<model::topic_id_partition> resolve_debug_topic_partitions(
  const cluster::topic_table& tt, const auto& proto_partitions) {
    chunked_vector<model::topic_id_partition> result;
    for (const auto& p : proto_partitions) {
        auto pid = model::partition_id{p.get_partition()};
        if (!p.get_topic_id().empty()) {
            auto uuid = model::topic_id{uuid_t::from_string(p.get_topic_id())};
            result.emplace_back(uuid, pid);
            continue;
        }
        if (p.get_topic().empty()) {
            continue;
        }
        const auto& topic_metadata = tt.get_topic_metadata_ref(
          model::topic_namespace{
            model::kafka_namespace, model::topic{p.get_topic()}});
        if (!topic_metadata) {
            throw serde::pb::rpc::not_found_exception(
              fmt::format("topic not found: {}", p.get_topic()));
        }
        auto topic_id = topic_metadata->get().get_configuration().tp_id;
        if (!topic_id) {
            throw serde::pb::rpc::not_found_exception(
              fmt::format("topic missing id: {}", p.get_topic()));
        }
        result.emplace_back(*topic_id, pid);
    }
    return result;
}

} // namespace

seastar::future<proto::admin::metastore::get_partition_summary_response>
metastore_service_impl::get_partition_summary(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::get_partition_summary_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .get_partition_summary(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    auto partitions = resolve_debug_topic_partitions(
      _topic_table->local(), req.get_partitions());

    using result_t = std::expected<
      chunked_vector<cloud_topics::l1::debug_reader::partition_summary>,
      cloud_topics::l1::rpc::errc>;

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp, partitions = std::move(partitions)](
        this auto,
        cloud_topics::l1::domain_supervisor& sup) -> ss::future<result_t> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->get_partition_summaries(std::move(partitions));
      });

    if (!result_exp.has_value()) {
        throw_rpc_errc(result_exp.error());
    }

    proto::admin::metastore::get_partition_summary_response response;
    for (const auto& s : result_exp.value()) {
        proto::admin::metastore::partition_summary ps;
        ps.set_topic_id(fmt::format("{}", s.tp.topic_id));
        ps.set_partition_id(s.tp.partition());
        ps.set_start_offset(s.metadata.start_offset());
        ps.set_next_offset(s.metadata.next_offset());
        ps.set_compaction_epoch(s.metadata.compaction_epoch());
        ps.set_extent_count(s.extent_count);
        ps.set_extent_min_offset(s.extent_min_offset());
        ps.set_extent_max_offset(s.extent_max_offset());
        ps.set_total_extent_data_size(s.total_extent_data_size);
        ps.set_term_count(s.term_count);
        ps.set_min_term_id(s.min_term());
        ps.set_max_term_id(s.max_term());
        ps.set_min_term_start_offset(s.min_term_start_offset());
        ps.set_max_term_start_offset(s.max_term_start_offset());
        ps.set_has_compaction_state(s.has_compaction_state);
        ps.set_cleaned_range_count(s.cleaned_range_count);
        ps.set_tombstone_range_count(s.tombstone_range_count);
        response.get_partitions().push_back(std::move(ps));
    }

    co_return response;
}

seastar::future<proto::admin::metastore::dump_partition_state_response>
metastore_service_impl::dump_partition_state(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::dump_partition_state_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .dump_partition_state(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    auto partitions = resolve_debug_topic_partitions(
      _topic_table->local(), req.get_partitions());
    bool include_objects = req.get_include_objects();
    bool check_existence = req.get_check_object_existence();

    using result_t = std::expected<
      cloud_topics::l1::domain_manager::dump_result,
      cloud_topics::l1::rpc::errc>;

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp,
       partitions = std::move(partitions),
       include_objects,
       check_existence](this auto, cloud_topics::l1::domain_supervisor& sup)
        -> ss::future<result_t> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->dump_partition_state(
            std::move(partitions), include_objects, check_existence);
      });

    if (!result_exp.has_value()) {
        throw_rpc_errc(result_exp.error());
    }

    proto::admin::metastore::dump_partition_state_response response;
    for (const auto& pd : result_exp.value().partitions) {
        proto::admin::metastore::partition_dump dump_proto;
        dump_proto.set_topic_id(fmt::format("{}", pd.tp.topic_id));
        dump_proto.set_partition_id(pd.tp.partition());

        proto::admin::metastore::partition_metadata meta;
        meta.set_start_offset(pd.metadata.start_offset());
        meta.set_next_offset(pd.metadata.next_offset());
        meta.set_compaction_epoch(pd.metadata.compaction_epoch());
        dump_proto.set_metadata(std::move(meta));

        for (const auto& ext : pd.extents) {
            proto::admin::metastore::extent_info ei;
            ei.set_base_offset(ext.base_offset());
            ei.set_last_offset(ext.last_offset());
            ei.set_max_timestamp(ext.max_timestamp());
            ei.set_filepos(ext.filepos);
            ei.set_len(ext.len);
            ei.set_object_id(fmt::format("{}", ext.oid));
            dump_proto.get_extents().push_back(std::move(ei));
        }

        for (const auto& ts : pd.term_starts) {
            proto::admin::metastore::term_start_info ti;
            ti.set_term_id(ts.term_id());
            ti.set_start_offset(ts.start_offset());
            dump_proto.get_term_starts().push_back(std::move(ti));
        }

        if (pd.compaction) {
            proto::admin::metastore::compaction_info ci;
            auto cleaned_vec = pd.compaction->cleaned_ranges.to_vec();
            for (const auto& r : cleaned_vec) {
                proto::admin::metastore::offset_range or_proto;
                or_proto.set_base_offset(r.base_offset());
                or_proto.set_last_offset(r.last_offset());
                ci.get_cleaned_ranges().push_back(std::move(or_proto));
            }
            for (const auto& t :
                 pd.compaction->cleaned_ranges_with_tombstones) {
                proto::admin::metastore::cleaned_range_with_tombstones ct;
                ct.set_base_offset(t.base_offset());
                ct.set_last_offset(t.last_offset());
                ct.set_cleaned_at_timestamp(t.cleaned_with_tombstones_at());
                ci.get_cleaned_ranges_with_tombstones().push_back(
                  std::move(ct));
            }
            dump_proto.set_compaction(std::move(ci));
        }

        response.get_partitions().push_back(std::move(dump_proto));
    }

    for (const auto& obj : result_exp.value().objects) {
        proto::admin::metastore::object_info oi;
        oi.set_object_id(fmt::format("{}", obj.oid));
        oi.set_total_data_size(obj.entry.total_data_size);
        oi.set_removed_data_size(obj.entry.removed_data_size);
        oi.set_footer_pos(obj.entry.footer_pos);
        oi.set_object_size(obj.entry.object_size);
        using src_existence
          = cloud_topics::l1::domain_manager::object_dump_entry::existence;
        using dst_existence = proto::admin::metastore::object_existence;
        switch (obj.exists_in_cloud) {
        case src_existence::exists:
            oi.set_exists_in_cloud(dst_existence::exists);
            break;
        case src_existence::missing:
            oi.set_exists_in_cloud(dst_existence::missing);
            break;
        case src_existence::check_failed:
            oi.set_exists_in_cloud(dst_existence::check_failed);
            break;
        case src_existence::unspecified:
            oi.set_exists_in_cloud(dst_existence::unspecified);
            break;
        }
        response.get_objects().push_back(std::move(oi));
    }

    co_return response;
}

seastar::future<proto::admin::metastore::check_partition_invariants_response>
metastore_service_impl::check_partition_invariants(
  serde::pb::rpc::context ctx,
  proto::admin::metastore::check_partition_invariants_request req) {
    model::ntp metastore_ntp{
      model::kafka_internal_namespace,
      model::l1_metastore_topic,
      model::partition_id{
        static_cast<model::partition_id::type>(req.get_metastore_partition())}};

    auto redirect_node = utils::redirect_to_leader(
      _metadata_cache->local(), metastore_ntp, _proxy_client.self_node_id());
    if (redirect_node) {
        co_return co_await _proxy_client
          .make_client_for_node<
            proto::admin::metastore::metastore_service_client>(*redirect_node)
          .check_partition_invariants(std::move(ctx), std::move(req));
    }

    auto shard = _shard_table->local().shard_for(metastore_ntp);
    if (!shard.has_value()) {
        throw serde::pb::rpc::unavailable_exception("no shard");
    }

    auto partitions = resolve_debug_topic_partitions(
      _topic_table->local(), req.get_partitions());
    bool check_existence = req.get_check_object_existence();

    using result_t = std::expected<
      chunked_vector<cloud_topics::l1::domain_manager::invariant_check_result>,
      cloud_topics::l1::rpc::errc>;

    auto result_exp = co_await _domain_supervisor->invoke_on(
      *shard,
      [metastore_ntp, partitions = std::move(partitions), check_existence](
        this auto,
        cloud_topics::l1::domain_supervisor& sup) -> ss::future<result_t> {
          auto dm = sup.get(metastore_ntp);
          if (!dm) {
              co_return std::unexpected(
                cloud_topics::l1::rpc::errc::not_leader);
          }
          co_return co_await dm->check_partition_invariants(
            std::move(partitions), check_existence);
      });

    if (!result_exp.has_value()) {
        throw_rpc_errc(result_exp.error());
    }

    proto::admin::metastore::check_partition_invariants_response response;
    for (const auto& r : result_exp.value()) {
        proto::admin::metastore::partition_invariant_result pr;
        pr.set_topic_id(fmt::format("{}", r.tp.topic_id));
        pr.set_partition_id(r.tp.partition());
        for (const auto& v : r.violations) {
            proto::admin::metastore::invariant_violation iv;
            iv.set_check_name(ss::sstring(v.check_name));
            iv.set_description(ss::sstring(v.description));
            pr.get_violations().push_back(std::move(iv));
        }
        response.get_partitions().push_back(std::move(pr));
    }

    co_return response;
}

} // namespace admin
