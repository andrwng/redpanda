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
#pragma once

#include "cluster/cloud_metadata/consumer_offsets_types.h"
#include "cluster/fwd.h"
#include "cluster/leader_routing_frontend.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "features/feature_table.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "outcome.h"
#include "rpc/fwd.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class offsets_recovery_router
  : public leader_routing_frontend<
      offsets_recovery_request,
      offsets_recovery_reply,
      offsets_recovery_client_protocol> {
    offsets_recovery_router(
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~offsets_recovery_router() = default;

    ss::future<result<rpc::client_context<offsets_recovery_reply>>> dispatch(
      offsets_recovery_client_protocol proto,
      offsets_recovery_request,
      model::timeout_clock::duration timeout) override;

    ss::future<offsets_recovery_reply>
    process(ss::shard_id, offsets_recovery_request req) override;

    offsets_recovery_reply error_resp(cluster::errc e) const override {
        return offsets_recovery_reply{{}, e};
    }

    ss::sstring process_name() const override { return "offsets recovery"; }
};

// Directs an offset recovery request to the leader of the NTP managing the
// requested groups.
class offsets_recovery_rpc_handler {
public:
    offsets_recovery_rpc_handler(
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      cluster::controller*,
      ss::sharded<features::feature_table>&,
      ss::sharded<kafka::coordinator_ntp_mapper>&);
};

} // namespace cluster::cloud_metadata
