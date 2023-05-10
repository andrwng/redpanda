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
#include "cluster/cloud_metadata/offsets_recovery_frontend.h"

#include "cluster/cloud_metadata/consumer_offsets_types.h"
#include "cluster/offsets_recovery_rpc_service.h"

namespace cluster::cloud_metadata {

offsets_recovery_router::offsets_recovery_router(
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<cluster::partition_leaders_table>& leaders,
  const model::node_id node_id,
  offsets_recovery_frontend& frontend)
  : leader_routing_frontend<
    offsets_recovery_request,
    offsets_recovery_reply,
    offsets_recovery_client_protocol>(
    shard_table,
    metadata_cache,
    connection_cache,
    leaders,
    model::kafka_consumer_offsets_nt,
    node_id,
    config::shard_local_cfg().metadata_dissemination_retries.value(),
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _frontend(frontend) {}

ss::future<offsets_recovery_reply> offsets_recovery_router::process(
  ss::shard_id shard_id, offsets_recovery_request req) {
    return _frontend.recover_offsets_on_shard(shard_id, std::move(req), 30s);
}

ss::future<result<rpc::client_context<offsets_recovery_reply>>>
offsets_recovery_router::dispatch(
  offsets_recovery_client_protocol proto,
  offsets_recovery_request req,
  model::timeout_clock::duration timeout) {
    return proto.offsets_recovery(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

offsets_recovery_frontend::offsets_recovery_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<cluster::partition_leaders_table>& leaders_table,
  model::node_id node_id,
  ss::sharded<kafka::coordinator_ntp_mapper>& group_ntp_mapper,
  ss::sharded<offsets_recovery_manager>& group_manager)
  : _ssg(ssg)
  , _recovery_router(
      shard_table,
      metadata_cache,
      connection_cache,
      leaders_table,
      node_id,
      *this)
  , _group_ntp_mapper(group_ntp_mapper)
  , _group_manager(group_manager) {}

cluster::errc offsets_recovery_frontend::find_partition_id_for_req(
  const offsets_recovery_request& req,
  model::partition_id& partition_id) const {
    if (req.group_data.empty()) {
        return cluster::errc::invalid_request;
    }
    auto offsets_ntp = _group_ntp_mapper.local().ntp_for(
      kafka::group_id(req.group_data[0].group_id));
    if (!offsets_ntp.has_value()) {
        return cluster::errc::topic_not_exists;
    }
    partition_id = offsets_ntp->tp.partition;
    return cluster::errc::success;
}

ss::future<offsets_recovery_reply>
offsets_recovery_frontend::recover_offsets_on_shard(
  ss::shard_id shard_id,
  offsets_recovery_request req,
  model::timeout_clock::duration) {
    return _group_manager.invoke_on(
      shard_id, [req = std::move(req)](auto& manager) {
          return manager.recover_offsets(std::move(req));
      });
}

ss::future<offsets_recovery_reply> offsets_recovery_frontend::recover_offsets(
  offsets_recovery_request req, model::timeout_clock::duration timeout) {
    if (req.group_data.empty()) {
        co_return offsets_recovery_reply{{}, cluster::errc::invalid_request};
    }
    auto offsets_ntp = _group_ntp_mapper.local().ntp_for(
      kafka::group_id(req.group_data[0].group_id));
    if (!offsets_ntp.has_value()) {
        co_return offsets_recovery_reply{{}, cluster::errc::topic_not_exists};
    }
    co_return co_await _recovery_router.process_or_dispatch(
      std::move(req), offsets_ntp->tp.partition, timeout);
}

} // namespace cluster::cloud_metadata
