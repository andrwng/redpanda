/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/server/handlers/delete_records.h"

#include "cluster/metadata_cache.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "model/fundamental.h"

#include <vector>

namespace kafka {

/// Returned in responses where kafka::error_code is anything else then a value
/// of error_code::none
static constexpr auto invalid_low_watermark = model::offset(-1);

/// Compare against user provided value of truncation offset, this value will
/// indicate to truncate at  the current partition high watermark
static constexpr auto current_high_watermark = model::offset(-1);

static std::vector<delete_records_partition_result>
make_partition_errors(const delete_records_topic& t, error_code ec) {
    std::vector<delete_records_partition_result> r;
    for (const auto& p : t.partitions) {
        r.push_back(delete_records_partition_result{
          .partition_index = p.partition_index,
          .low_watermark = invalid_low_watermark,
          .error_code = ec});
    }
    return r;
}

/// Performs validation of topics, any failures will result in a list of
/// partitions that all contain the identical error codes
static std::vector<delete_records_partition_result>
validate_at_topic_level(request_context& ctx, const delete_records_topic& t) {
    const auto is_authorized = [&ctx](const delete_records_topic& t) {
        return ctx.authorized(security::acl_operation::remove, t.name);
    };
    const auto is_deletable = [&ctx](const delete_records_topic& t) {
        const auto cfg = ctx.metadata_cache().get_topic_cfg(
          model::topic_namespace_view(model::kafka_namespace, t.name));
        if (!cfg || !cfg->properties.cleanup_policy_bitflags) {
            return false;
        }
        /// TODO: call is_collectable()? Maybe theres overrides
        const auto flags = cfg->properties.cleanup_policy_bitflags;
        return (*flags & model::cleanup_policy_bitflags::deletion)
               == model::cleanup_policy_bitflags::deletion;
    };
    const auto is_cloud_enabled = [&ctx](const delete_records_topic& t) {
        const auto cfg = ctx.metadata_cache().get_topic_cfg(
          model::topic_namespace_view(model::kafka_namespace, t.name));
        if (!cfg) {
            return false;
        }
        const auto si_flags = cfg->properties.shadow_indexing;
        return si_flags && *si_flags != model::shadow_indexing_mode::disabled;
    };
    const auto is_internal_topic = [](const delete_records_topic& t) {
        const auto& internal_topics
          = config::shard_local_cfg().kafka_nodelete_topics();
        return std::find_if(
                 internal_topics.begin(),
                 internal_topics.end(),
                 [t](const ss::sstring& name) { return name == t.name; })
               != internal_topics.end();
    };

    if (!is_authorized(t)) {
        return make_partition_errors(
          t, error_code::cluster_authorization_failed);
    } else if (!is_deletable(t)) {
        return make_partition_errors(t, error_code::policy_violation);
    } else if (is_cloud_enabled(t) || is_internal_topic(t)) {
        return make_partition_errors(t, error_code::invalid_topic_exception);
    }
    return {};
}

/// Result set includes topic for later group-by topic logic
using result_t = std::tuple<model::topic, delete_records_partition_result>;

static result_t make_partition_error(const model::ntp& ntp, error_code err) {
    return std::make_tuple(
      ntp.tp.topic,
      delete_records_partition_result{
        .partition_index = ntp.tp.partition,
        .low_watermark = invalid_low_watermark,
        .error_code = err});
}

static result_t
make_partition_response(const model::ntp& ntp, model::offset low_watermark) {
    return std::make_tuple(
      ntp.tp.topic,
      delete_records_partition_result{
        .partition_index = ntp.tp.partition,
        .low_watermark = low_watermark,
        .error_code = error_code::none});
}

/// If validation passes, attempts to prefix truncate the raft log at the given
/// offset. Returns a response that includes the new low watermark
static ss::future<result_t> prefix_truncate(
  cluster::partition_manager& pm,
  model::ntp ntp,
  model::offset truncation_offset,
  std::chrono::milliseconds timeout_ms) {
    auto partition = pm.get(ntp);
    if (!partition) {
        co_return make_partition_error(
          ntp, error_code::unknown_topic_or_partition);
    }
    if (!partition->is_leader()) {
        co_return make_partition_error(
          ntp, error_code::not_leader_for_partition);
    }
    if (truncation_offset == current_high_watermark) {
        /// User is requesting to truncate all data
        truncation_offset = partition->high_watermark();
    }
    if (truncation_offset < model::offset(0)) {
        co_return make_partition_error(ntp, error_code::offset_out_of_range);
    }

    /// Perform truncation at the requested offset. A special batch will be
    /// written to the log, eventually consumed by replicas via the
    /// new_log_eviction_stm, which will perform a prefix truncation at the
    /// given offset
    auto errc = co_await partition->prefix_truncate(
      truncation_offset, ss::lowres_clock::now() + timeout_ms);
    if (errc) {
        vassert(
          errc.category() == cluster::error_category(),
          "Unknown error category observed");
        auto kerr = error_code::unknown_server_error;
        switch (cluster::errc(errc.value())) {
        case cluster::errc::timeout:
        case cluster::errc::shutting_down:
            kerr = error_code::request_timed_out;
            break;
        case cluster::errc::invalid_truncation_offset:
            kerr = error_code::offset_out_of_range;
            break;
        case cluster::errc::feature_disabled:
            vlog(
              klog.warn,
              "delete_records request rejected - feature not yet activated");
        default:
            kerr = error_code::unknown_server_error;
        }
        vlog(
          klog.info,
          "Possible failed attempted to truncate partition: {} error: {}",
          ntp,
          kerr);
        co_return make_partition_error(ntp, kerr);
    }
    const auto kafka_start_offset
      = partition->get_offset_translator_state()->from_log_offset(
        partition->start_offset());
    vlog(
      klog.info,
      "Truncated partition: {} to log offset: {} kafka offset: {}",
      ntp,
      partition->start_offset(),
      kafka_start_offset);
    /// prefix_truncate() will return when the start_offset has been incremented
    /// to the desired new low watermark. No other guarantees about the system
    /// are made at this point. (i.e. if data on disk on this node or a replica
    /// has yet been deleted)
    co_return make_partition_response(ntp, kafka_start_offset);
}

template<>
ss::future<response_ptr>
delete_records_handler::handle(request_context ctx, ss::smp_service_group) {
    delete_records_request request;
    request.decode(ctx.reader(), ctx.header().version);
    log_request(ctx.header(), request);

    delete_records_response response;
    std::vector<ss::future<result_t>> fs;
    for (auto& topic : request.data.topics) {
        /// Topic level validation, errors will be all the same for each
        /// partition under the topic. Validation for individual partitions may
        /// happen in the inner for loop below.
        auto topic_level_errors = validate_at_topic_level(ctx, topic);
        if (!topic_level_errors.empty()) {
            response.data.topics.push_back(delete_records_topic_result{
              .name = topic.name, .partitions = std::move(topic_level_errors)});
            continue;
        }
        for (auto& partition : topic.partitions) {
            auto ntp = model::ntp(
              model::kafka_namespace,
              model::topic_partition(topic.name, partition.partition_index));
            auto shard = ctx.shards().shard_for(ntp);
            if (!shard) {
                fs.push_back(
                  ss::make_ready_future<result_t>(make_partition_error(
                    ntp, error_code::unknown_topic_or_partition)));
                continue;
            }
            auto f
              = ctx.partition_manager()
                  .invoke_on(
                    *shard,
                    [ntp,
                     timeout = request.data.timeout_ms,
                     o = partition.offset](cluster::partition_manager& pm) {
                        return prefix_truncate(pm, ntp, o, timeout);
                    })
                  .handle_exception([ntp](std::exception_ptr eptr) {
                      vlog(klog.error, "Caught unexpected exception: {}", eptr);
                      return make_partition_error(
                        ntp, error_code::unknown_server_error);
                  });
            fs.push_back(std::move(f));
        }
    }

    /// Perform prefix truncation on partitions
    auto results = co_await ss::when_all_succeed(fs.begin(), fs.end());

    /// Group results by topic
    using partition_results = std::vector<delete_records_partition_result>;
    absl::flat_hash_map<model::topic, partition_results> group_by_topic;
    for (auto& [name, partitions] : results) {
        group_by_topic[name].push_back(std::move(partitions));
    }

    /// Map to kafka response type
    for (auto& [topic, partition_results] : group_by_topic) {
        response.data.topics.push_back(delete_records_topic_result{
          .name = topic, .partitions = std::move(partition_results)});
    }
    co_return co_await ctx.respond(std::move(response));
}
} // namespace kafka
