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
#include "cluster/cloud_metadata/offsets_recovery_rpc_handler.h"
#include "cluster/cloud_metadata/consumer_offsets_types.h"
#include "cluster/offsets_recovery_rpc_service.h"

namespace cluster::cloud_metadata {

offsets_recovery_router::offsets_recovery_router(
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
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
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value()) {}

ss::future<offsets_recovery_reply>
offsets_recovery_router::process(ss::shard_id, offsets_recovery_request) {
    co_return offsets_recovery_reply{};
}

ss::future<result<rpc::client_context<offsets_recovery_reply>>>
offsets_recovery_router::dispatch(
  offsets_recovery_client_protocol proto,
  offsets_recovery_request req,
  model::timeout_clock::duration timeout) {
    return proto.offsets_recovery(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

} // namespace cluster::cloud_metadata
