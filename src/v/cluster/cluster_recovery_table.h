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

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/controller_snapshot.h"
#include "cluster/types.h"

#include <seastar/core/chunked_fifo.hh>

namespace cluster {

// Tracks the state of an on-going cluster recovery. This only exposes an
// interface to expose and modify the current status of recovery; an external
// caller should use these to actually drive recovery.
//
// This only tracks the parts of recovery that are not already tracked by
// another subsystem of the controller. I.e. topics, configs, etc. are tracked
// in other controller tables; but consumer offsets pending recovery are
// tracked here.
struct cluster_recovery_state {
public:
    enum class status {
        // A recovery has been initialized.
        // - We've already downloaded and serialized the manifest.
        // - While in this state, a recovery manager may validate that the
        //   recovery materials are downloadable.
        initialized = 0,

        // Recovery steps are beginning.
        // - We've already validated that the recovery materials are
        //   downloadable, though these aren't persisted in the controller
        //   beyond the manifest (it is expected that upon leadership changes,
        //   they are redownloaded).
        // - While in this state, a recovery manager may reconcile the cluster
        //   with the state in the recovery materials.
        starting = 1,

        // Recovery has completed successfully. This is a terminal state.
        complete = 2,

        // Recovery has failed. This is a terminal state.
        failed = 3,
    };

    cluster_recovery_state() = delete;

    explicit cluster_recovery_state(
      cloud_metadata::cluster_metadata_manifest manifest)
      : manifest(std::move(manifest)) {}

    bool is_active() const {
        return !(status == status::complete || status == status::failed);
    }

    // Current state of this recovery.
    status status{status::initialized};

    // Manifest that defines the desired end state of this recovery.
    cloud_metadata::cluster_metadata_manifest manifest{};

    // Only applicable when failed.
    std::optional<ss::sstring> error_msg;
};

// Tracks the state of recovery attempts performed on the cluster.
//
// It is expected this will live on every shard on every node, making it easy
// to determine from any shard what the current status of recovery is (e.g. for
// reporting, or to install a guardrail while recovery is running).
class cluster_recovery_table {
public:
    bool is_recovery_active() const {
        if (_states.empty()) {
            return false;
        }
        return _states.back().is_active();
    }

    std::error_code apply(model::offset offset, cluster_recovery_init_cmd);
    std::error_code apply(model::offset offset, cluster_recovery_start_cmd);
    std::error_code apply(model::offset offset, cluster_recovery_stop_cmd);

    // TODO: fill me in!
    void fill_snapshot(controller_snapshot&) {}
    void apply_snapshot(model::offset, const controller_snapshot&) {}

private:
    ss::chunked_fifo<cluster_recovery_state> _states;
};

} // namespace cluster
