// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_discovery.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

cluster_discovery::cluster_discovery(
  const config::node_config& node_config, const model::node_uuid& node_uuid)
  : _node_config(node_config)
  , _node_uuid(node_uuid) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    const auto& configured_node_id = _node_config.node_id();
    if (configured_node_id == -1) {
        clusterlog.info("Using configured node ID {}", configured_node_id);
        co_return configured_node_id;
    }
    // TODO: once is_cluster_founder() refers to all seeds, verify that all the
    // seeds' seed_servers lists match and assign node IDs based on the
    // ordering.
    if (is_cluster_founder()) {
        // If this is the root node, assign node 0.
        co_return model::node_id(0);
    }
    auto node_id = co_await request_node_id_for_uuid();
    co_return node_id;
}

vector<broker> cluster_discovery::initial_seed_brokers() {
    // If configured as the root node, we'll want to start the cluster with
    // just this node as the initial seed.
    if (is_cluster_founder()) {
        // TODO: we should only return non-empty seed list if our log is empty.
        return {make_self_broker(_node_config)};
    }
    return {};
}

ss::future<ss::stop_iteration>
cluster_discovery::attempt_node_uuid_registration(
  model::node_id& assigned_node_id) {
    const auto& seed_servers = _node_config.seed_servers();
    auto self = make_self_broker(_node_config);
    for (const auto& s : seed_servers) {
        vlog(
          clusterlog.info,
          "Requesting node ID for UUID {} from {}",
          _node_uuid,
          s.addr);
        auto reply_result
          = co_await do_with_client_one_shot<controller_client_protocol>(
            s.addr,
            _node_config.rpc_server_tls(),
            2s,
            [&self, this](controller_client_protocol c) {
                return c
                  .join_node(
                    join_node_request(
                      features::feature_table::get_latest_logical_version(),
                      _node_uuid.to_vector(),
                      self),
                    rpc::client_opts(rpc::clock_type::now() + 2s))
                  .then(&rpc::get_ctx_data<join_node_reply>);
            });
        if (!reply_result) {
            continue;
        }
        const auto& reply = reply_result.value();
        if (!reply.success) {
            continue;
        }
        if (reply.id < 0) {
            // Something else went wrong. Maybe duplicate UUID?
            continue;
        }
        assigned_node_id = reply.id;
        co_return ss::stop_iteration::yes;
    }
    ss::abort_source as;
    co_await ss::sleep_abortable(std::chrono::milliseconds(1000), as)
      .handle_exception_type([](const ss::sleep_aborted&) {
          // XXX: log
      });
    co_return ss::stop_iteration::no;
}

ss::future<model::node_id> cluster_discovery::request_node_id_for_uuid() {
    model::node_id assigned_node_id;
    co_await ss::repeat([this, &assigned_node_id] {
        return attempt_node_uuid_registration(assigned_node_id);
    });
    co_return assigned_node_id;
}

bool cluster_discovery::is_cluster_founder() const {
    return _node_config.seed_servers().empty();
}

} // namespace cluster
