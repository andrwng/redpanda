/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cluster_recovery_manager.h"

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "seastarx.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

cluster_recovery_manager::cluster_recovery_manager(
  ss::sharded<ss::abort_source>& sharded_as,
  ss::sharded<cloud_storage::remote>& remote,
  ss::sharded<cluster_recovery_table>& recovery_table,
  ss::sharded<controller_stm>& controller_stm,
  ss::sharded<features::feature_table>& feature_table)
  : _sharded_as(sharded_as)
  , _remote(remote)
  , _recovery_table(recovery_table)
  , _controller_stm(controller_stm)
  , _feature_table(feature_table) {}

ss::future<bool> cluster_recovery_manager::initialize_recovery() {
    if (_recovery_table.local().is_recovery_active()) {
        co_return false;
    }
    auto units = co_await ss::get_units(_sem, 1, _sharded_as.local());
    if (_recovery_table.local().is_recovery_active()) {
        co_return false;
    }
    // TODO: Download the manifest.
    cloud_metadata::cluster_metadata_manifest manifest;

    // Replicate an update to start recovery. Once applied, this will update
    // the recovery table.
    cluster_recovery_init_cmd_data data;
    data.manifest = std::move(manifest);
    auto errc = co_await replicate_and_wait(
      _controller_stm,
      _feature_table,
      _sharded_as,
      cluster_recovery_init_cmd(0, std::move(data)),
      model::timeout_clock::now() + 30s);
    if (errc != errc::success) {
        co_return false;
    }
    co_return true;
}

ss::future<std::error_code> cluster_recovery_manager::apply_to_table(
  model::offset offset, cluster_recovery_init_cmd cmd) {
    return dispatch_updates_to_cores(offset, std::move(cmd));
}
ss::future<std::error_code> cluster_recovery_manager::apply_to_table(
  model::offset offset, cluster_recovery_start_cmd cmd) {
    return dispatch_updates_to_cores(offset, std::move(cmd));
}
ss::future<std::error_code> cluster_recovery_manager::apply_to_table(
  model::offset offset, cluster_recovery_stop_cmd cmd) {
    return dispatch_updates_to_cores(offset, std::move(cmd));
}

template<typename Cmd>
ss::future<std::error_code> cluster_recovery_manager::dispatch_updates_to_cores(
  model::offset offset, Cmd cmd) {
    return _recovery_table
      .map([cmd, offset](auto& local_table) {
          return local_table.apply(offset, cmd);
      })
      .then([](std::vector<std::error_code> results) {
          auto first_res = results.front();
          auto state_consistent = std::all_of(
            results.begin(), results.end(), [first_res](std::error_code res) {
                return first_res == res;
            });

          vassert(
            state_consistent,
            "State inconsistency across shards detected, "
            "expected result: {}, have: {}",
            first_res,
            results);

          return first_res;
      });
}

} // namespace cluster
