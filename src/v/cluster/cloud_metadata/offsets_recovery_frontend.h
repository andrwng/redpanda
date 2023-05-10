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
#include "cluster/cloud_metadata/offsets_recovery_manager.h"
#include "cluster/fwd.h"
#include "cluster/leader_routing_frontend.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "features/feature_table.h"
#include "kafka/server/coordinator_ntp_mapper.h"
#include "kafka/server/group_manager.h"
#include "outcome.h"
#include "rpc/fwd.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class offsets_recovery_frontend;

class offsets_recovery_router
  : public cluster::leader_routing_frontend<
      offsets_recovery_request,
      offsets_recovery_reply,
      offsets_recovery_client_protocol> {
public:
    offsets_recovery_router(
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<cluster::partition_leaders_table>&,
      const model::node_id,
      offsets_recovery_frontend&);
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

private:
    offsets_recovery_frontend& _frontend;
};

// Entrypoint for callers within a Redpanda process to send requests for
// offsets recovery.
class offsets_recovery_frontend {
public:
    offsets_recovery_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<cluster::partition_leaders_table>&,
      model::node_id,
      ss::sharded<kafka::coordinator_ntp_mapper>&,
      ss::sharded<offsets_recovery_manager>&);

    ss::future<offsets_recovery_reply> recover_offsets(
      offsets_recovery_request req, model::timeout_clock::duration timeout);

    ss::future<offsets_recovery_reply> recover_offsets_on_shard(
      ss::shard_id, offsets_recovery_request, model::timeout_clock::duration);

    offsets_recovery_router& recovery_router() { return _recovery_router; }

    cluster::errc find_partition_id_for_req(
      const offsets_recovery_request&, model::partition_id&) const;

    ss::future<offsets_recovery_reply>
    recover_on_shard(ss::shard_id shard_id, offsets_recovery_request req);

private:
    ss::smp_service_group _ssg;
    offsets_recovery_router _recovery_router;
    ss::sharded<kafka::coordinator_ntp_mapper>& _group_ntp_mapper;
    ss::sharded<offsets_recovery_manager>& _group_manager;
};

} // namespace cluster::cloud_metadata
