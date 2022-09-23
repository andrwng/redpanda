// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

#include <optional>
#include <vector>

namespace config {
struct node_config;
} // namespace config

namespace cluster {

// Provides metadata pertaining to initial cluster discovery.
class cluster_discovery {
public:
    cluster_discovery(
      const config::node_config& node_config,
      const model::node_uuid& node_uuid);

    // TODO: below is not accurate yet. Implement me!
    //
    // Determines what the node ID for this node should be. Once called, we can
    // proceed with initializing anything that depends on node ID.
    //
    // On an empty seed server, this sends requests to all other seed servers
    // to determine if there is a valid assigned of node IDs for the seeds.
    //
    // On a non-seed server with no node ID specified via config, this sends a
    // request to the controllers to register this node's UUID and assign it a
    // node ID.
    ss::future<model::node_id> determine_node_id();

    // TODO: below is not accurate yet. Implement me!
    //
    // Returns brokers to be used to form a Raft group for a new cluster.
    //
    // If this node is not a seed server, or if a cluster has already been
    // formed, returns an empty list.
    std::vector<model::broker> initial_seed_brokers();

private:
    // TODO: below is not accurate yet. Implement me!
    //
    // Returns true if the local node is a founding member of the cluster, as
    // indicated by either us having an empty seed server (we are the root node
    // in a legacy config) or our node UUID matching one of those returned by
    // the seed servers.
    bool is_cluster_founder() const;

    // Local configuration of this node.
    const config::node_config& _node_config;
    const model::node_uuid _node_uuid;
};

} // namespace cluster
