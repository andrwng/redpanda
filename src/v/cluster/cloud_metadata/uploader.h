/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/types.h"
#include "config/property.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace cloud_storage {
class remote;
} // namespace cloud_storage

namespace cluster::cloud_metadata {

// Periodically uploads cluster metadata within a given term.
//
// It is expected that this is instantiated on all controller replicas. Unlike
// other Raft-driven loops (e.g. the NTP archiver), this is not driven by
// replicating messages via Raft (e.g. archival_metadata_stm). Instead, this
// uploader uses Raft linearizable barriers to send heartbeats to followers and
// assert it is still the leader before performing operations.
//
// Since there is no Raft-replicated state machine that would be replicated on
// all nodes, only the leader uploader keeps an in-memory view of cluster
// metadata. Upon becoming leader, this view is hydrated from remote storage.
class uploader {
public:
    uploader(
      model::cluster_uuid cluster_uuid,
      cloud_storage_clients::bucket_name bucket,
      cloud_storage::remote& remote,
      consensus_ptr raft0);

    ss::future<> stop_and_wait();

    // Periodically uploads cluster metadata for as long as the local
    // controller replica is the leader.
    //
    // At most one invocation should be running at any given time.
    ss::future<> upload_until_term_change();

private:
    // Returns true if we're no longer the leader or the term has changed since
    // the input term.
    ss::future<bool> term_has_changed(model::term_id);

    const model::cluster_uuid _cluster_uuid;
    cloud_storage::remote& _remote;
    consensus_ptr _raft0;
    const cloud_storage_clients::bucket_name _bucket;

    config::binding<std::chrono::milliseconds> _upload_interval_ms;

    // Protect the lifecycle of the members below.
    ss::gate _gate;
    ss::abort_source _as;

    // Abort source to stop sleeping if there is a term change.
    std::optional<std::reference_wrapper<ss::abort_source>> _term_as;

    // Used to wait for leadership. It will be triggered by notify_leadership.
    ss::condition_variable _leader_cond;
};

} // namespace cluster::cloud_metadata
