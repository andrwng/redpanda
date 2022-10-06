/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/bootstrap_backend.h"

#include "cluster/commands.h"
#include "cluster/logger.h"
#include "cluster/types.h"

namespace cluster {

bootstrap_backend::bootstrap_backend(
  const std::optional<cluster_uuid>& cluster_uuid,
  ss::sharded<storage::api>& storage)
  : _cluster_uuid(cluster_uuid)
  , _storage(storage) {}

ss::future<std::error_code>
bootstrap_backend::apply_update(model::record_batch b) {
    vlog(clusterlog.info, "Applying update to bootstrap_manager");

    // handle node managements command
    static constexpr auto accepted_commands
      = make_commands_list<bootstrap_cluster_cmd>();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd, [this](bootstrap_cluster_cmd cmd) -> ss::future<std::error_code> {
          if (_cluster_uuid.has_value()) {
              vlog(
                clusterlog.debug,
                "Skipping bootstrap_cluster_cmd {}, current cluster_uuid: {}",
                cmd.value.uuid,
                *_cluster_uuid);
              co_return errc::success;
          }

          _cluster_uuid = cmd.value.uuid;
          co_await write_stored_cluster_uuid(_storage.local(), cmd.value.uuid);
          vlog(clusterlog.info, "Cluster {} created", cmd.value.uuid);
          co_return errc::success;
      });
}

} // namespace cluster
