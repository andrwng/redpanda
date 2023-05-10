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
#include "kafka/server/group_manager.h"
#include "outcome.h"
#include "rpc/fwd.h"
#include "seastarx.h"

#include <seastar/core/sharded.hh>

namespace cluster::cloud_metadata {

class offsets_recovery_frontend;

// Directs an offset recovery request to the leader of the NTP managing the
// requested groups.
class offsets_recovery_rpc_handler : public offsets_recovery_service {
public:
    offsets_recovery_rpc_handler(
      ss::scheduling_group,
      ss::smp_service_group,
      ss::sharded<offsets_recovery_frontend>&);

    ss::future<offsets_recovery_reply> virtual recover_offsets(
      offsets_recovery_request&&, rpc::streaming_context&) final;

private:
    ss::sharded<offsets_recovery_frontend>& _frontend;
};

} // namespace cluster::cloud_metadata
