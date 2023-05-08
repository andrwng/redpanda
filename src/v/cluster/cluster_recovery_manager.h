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

#include "cloud_storage/remote.h"
#include "cluster/cluster_recovery_table.h"
#include "cluster/commands.h"
#include "cluster/fwd.h"
#include "seastarx.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>

namespace cluster {

class cluster_recovery_manager {
public:
    cluster_recovery_manager(
      ss::sharded<ss::abort_source>&,
      ss::sharded<cloud_storage::remote>&,
      ss::sharded<cluster_recovery_table>&,
      ss::sharded<controller_stm>&,
      ss::sharded<features::feature_table>&);

    // Starts a recovery if one isn't already in progress.
    ss::future<bool> initialize_recovery();

    ss::future<> fill_snapshot(controller_snapshot&) const;
    ss::future<> apply_snapshot(model::offset, const controller_snapshot&);

    ss::future<std::error_code> apply_update(model::record_batch b);

private:
    static constexpr auto commands = make_commands_list<
      cluster_recovery_init_cmd,
      cluster_recovery_start_cmd,
      cluster_recovery_stop_cmd>();
    bool is_batch_applicable(const model::record_batch& b) const;


    // Sends updates to each shard of the underlying recovery table.
    template<typename Cmd>
    ss::future<std::error_code>
    dispatch_updates_to_cores(model::offset, Cmd cmd);

    ss::future<std::error_code>
      apply_to_table(model::offset, cluster_recovery_init_cmd);
    ss::future<std::error_code>
      apply_to_table(model::offset, cluster_recovery_start_cmd);
    ss::future<std::error_code>
      apply_to_table(model::offset, cluster_recovery_stop_cmd);

    ss::gate _gate;
    // Ensures only one update to the recovery table at once.
    ssx::named_semaphore<ss::lowres_clock> _sem{1, "cluster_recovery"};

    ss::sharded<ss::abort_source>& _sharded_as;

    // Remote with which to download recovery materials.
    ss::sharded<cloud_storage::remote>& _remote;

    // State that backs the recoveries managed by this manager.
    ss::sharded<cluster_recovery_table>& _recovery_table;

    // Controller state used to drive controller commands.
    ss::sharded<controller_stm>& _controller_stm;
    ss::sharded<features::feature_table>& _feature_table;
};

} // namespace cluster
