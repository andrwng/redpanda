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
#include "cluster/cloud_metadata/offsets_recovery_frontend.h"
#include "cluster/offsets_recovery_rpc_service.h"

namespace cluster::cloud_metadata {

offsets_recovery_rpc_handler::offsets_recovery_rpc_handler(
  ss::scheduling_group sg,
  ss::smp_service_group ssg,
  ss::sharded<offsets_recovery_frontend>& frontend)
  : offsets_recovery_service(sg, ssg)
  , _frontend(frontend) {}

ss::future<offsets_recovery_reply>
offsets_recovery_rpc_handler::recover_offsets(
  offsets_recovery_request&& req, rpc::streaming_context& ctx) {
    model::partition_id pid{};
    auto err = _frontend.local().find_partition_id_for_req(req, pid);
    if (err != cluster::errc::success) {
        co_return offsets_recovery_reply{{}, err};
    }
    co_return co_await _frontend.local()
      .recovery_router()
      .find_shard_and_process(
        std::forward<offsets_recovery_request>(req), pid, 30s);
}

} // namespace cluster::cloud_metadata
