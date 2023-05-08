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
#include "cluster/cluster_recovery_table.h"

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/logger.h"

namespace cluster {

std::error_code cluster_recovery_table::apply(
  model::offset offset, cluster_recovery_init_cmd cmd) {
    if (!_states.empty() && _states.back().is_active()) {
        return errc::update_in_progress;
    }
    vlog(
      clusterlog.info,
      "Initializing cluster recovery at offset {} with manifest: {}",
      offset,
      cmd.value.manifest);
    _states.emplace_back(std::move(cmd.value.manifest));
    return errc::success;
}

std::error_code
cluster_recovery_table::apply(model::offset, cluster_recovery_start_cmd) {
    if (
      _states.empty()
      || _states.back().status != cluster_recovery_state::status::initialized) {
        return errc::invalid_request;
    }
    vlog(clusterlog.info, "Starting cluster recovery");
    _states.back().status = cluster_recovery_state::status::starting;
    return errc::success;
}

std::error_code cluster_recovery_table::apply(
  model::offset offset, cluster_recovery_stop_cmd cmd) {
    if (_states.empty() || !_states.back().is_active()) {
        return errc::invalid_request;
    }
    if (cmd.value.error_msg.has_value()) {
        vlog(
          clusterlog.warn,
          "Stopping cluster recovery with error message: {}",
          cmd.value.error_msg.value());
        _states.back().status = cluster_recovery_state::status::failed;
        _states.back().error_msg = std::move(cmd.value.error_msg);
        return errc::success;
    }
    vlog(
      clusterlog.info,
      "Marking cluster recovery as complete at offset {}",
      offset);
    _states.back().status = cluster_recovery_state::status::complete;
    return errc::success;
}

} // namespace cluster
