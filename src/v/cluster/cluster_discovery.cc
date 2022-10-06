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
#include "storage/kvstore.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

cluster_discovery::cluster_discovery(
  const model::node_uuid& node_uuid,
  storage::kvstore& kvstore,
  ss::abort_source& as)
  : _node_uuid(node_uuid)
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _kvstore(kvstore)
  , _as(as) {}

ss::future<node_id> cluster_discovery::determine_node_id() {
    // TODO: read from disk if empty.
    const auto& configured_node_id = config::node().node_id();
    if (configured_node_id != std::nullopt) {
        clusterlog.info("Using configured node ID {}", configured_node_id);
        co_return *configured_node_id;
    }
    static const bytes invariants_key("configuration_invariants");
    auto invariants_buf = _kvstore.get(
      storage::kvstore::key_space::controller, invariants_key);
    if (invariants_buf) {
        auto invariants = reflection::from_iobuf<configuration_invariants>(
          std::move(*invariants_buf));
        co_return invariants.node_id;
    }

    if (auto cf_node_id = get_cluster_founder_node_id(); cf_node_id) {
        // TODO: verify that all the seeds' seed_servers lists match
        clusterlog.info("Using index based node ID {}", *cf_node_id);
        co_return *cf_node_id;
    }
    model::node_id assigned_node_id;
    co_await ss::repeat([this, &assigned_node_id] {
        return dispatch_node_uuid_registration_to_seeds(assigned_node_id);
    });
    co_return assigned_node_id;
}

vector<broker>
cluster_discovery::initial_seed_brokers(const bool cluster_exists) const {
    // If the cluster has been formed, return empty
    if (cluster_exists) return {};
    // If configured as the root node, we'll want to start the cluster with
    // just this node as the initial seed.
    if (config::node().empty_seed_starts_cluster()) {
        if (config::node().seed_servers().empty())
        return {make_self_broker(config::node())};
        // Not a root
        return {};
    }
    if (get_node_index_in_seed_servers()) {
        // TODO: return the discovered nodes plus this node
        return {make_self_broker(config::node())};
    }
    // Non-seed server
    return {};
}

ss::future<ss::stop_iteration>
cluster_discovery::dispatch_node_uuid_registration_to_seeds(
  model::node_id& assigned_node_id) {
    const auto& seed_servers = config::node().seed_servers();
    auto self = make_self_broker(config::node());
    for (const auto& s : seed_servers) {
        vlog(
          clusterlog.info,
          "Requesting node ID for UUID {} from {}",
          _node_uuid,
          s.addr);
        result<join_node_reply> r(join_node_reply{});
        try {
            r = co_await do_with_client_one_shot<controller_client_protocol>(
              s.addr,
              config::node().rpc_server_tls(),
              _join_timeout,
              [&self, this](controller_client_protocol c) {
                  return c
                    .join_node(
                      join_node_request(
                        features::feature_table::get_latest_logical_version(),
                        _node_uuid().to_vector(),
                        self),
                      rpc::client_opts(rpc::clock_type::now() + _join_timeout))
                    .then(&rpc::get_ctx_data<join_node_reply>);
              });
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}, retrying: {}",
              _node_uuid,
              std::current_exception());
            continue;
        }
        if (!r || r.has_error() || !r.value().success) {
            vlog(
              clusterlog.debug,
              "Error registering UUID {}, retrying",
              _node_uuid);
            continue;
        }
        const auto& reply = r.value();
        if (reply.id < 0) {
            // Something else went wrong. Maybe duplicate UUID?
            vlog(clusterlog.debug, "Negative node ID {}", reply.id);
            continue;
        }
        assigned_node_id = reply.id;
        co_return ss::stop_iteration::yes;
    }
    co_await ss::sleep_abortable(_join_retry_jitter.next_duration(), _as);
    co_return ss::stop_iteration::no;
}

/*static*/ std::optional<node_id>
cluster_discovery::get_cluster_founder_node_id() {
    if (config::node().empty_seed_starts_cluster()) {
        if (config::node().seed_servers().empty())
            return node_id{0};
        else
            return {};
    } else {
        if (auto idx = get_node_index_in_seed_servers(); idx)
            return node_id{*idx};
        else
            return {};
    }
}

/*static*/ std::optional<int32_t>
cluster_discovery::get_node_index_in_seed_servers() {
    const std::vector<config::seed_server>& seed_servers
      = config::node().seed_servers();
    vassert(
      !seed_servers.empty(),
      "Configuration error: seed_servers cannot be empty when "
      "empty_seed_starts_cluster is false");
    const auto it = std::find_if(
      seed_servers.cbegin(),
      seed_servers.cend(),
      [rpc_addr = config::node().advertised_rpc_api()](
        const config::seed_server& seed_server) {
          return rpc_addr == seed_server.addr;
      });
    if (it == seed_servers.cend()) return {};
    return {
      boost::numeric_cast<int32_t>(std::distance(seed_servers.cbegin(), it))};
}

} // namespace cluster
