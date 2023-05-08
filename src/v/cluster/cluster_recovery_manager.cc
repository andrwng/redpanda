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
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "config/configuration.h"
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
    auto retry_node = retry_chain_node{
      _sharded_as.local(),
      config::shard_local_cfg().cloud_metadata_download_timeout_ms(),
      config::shard_local_cfg().cloud_metadata_download_backoff_ms()};
    const auto bucket = cloud_storage_clients::bucket_name{
      config::shard_local_cfg().cloud_storage_bucket().value()};
    auto cluster_manifest
      = co_await cluster::cloud_metadata::find_latest_manifest(
        _remote.local(), bucket, retry_node);

    if (!cluster_manifest.has_value()) {
        co_return false;
    }

    // Replicate an update to start recovery. Once applied, this will update
    // the recovery table.
    cluster_recovery_init_cmd_data data;
    data.manifest = std::move(cluster_manifest.value());
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

ss::future<std::error_code>
cluster_recovery_manager::apply_update(model::record_batch b) {
    auto offset = b.base_offset();
    auto cmd = co_await cluster::deserialize(std::move(b), commands);
    co_return co_await ss::visit(
      [this, offset](auto cmd) { return apply_to_table(offset, std::move(cmd)); },
      std::move(cmd));
}
bool cluster_recovery_manager::is_batch_applicable(const model::record_batch& b) const {
    return b.header().type == model::record_batch_type::cluster_recovery_cmd;
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
