// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_discovery.h"

#include "cluster/bootstrap_types.h"
#include "cluster/cluster_bootstrap_service.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_service.h"
#include "cluster/logger.h"
#include "config/node_config.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "seastarx.h"
#include "storage/kvstore.h"

#include <chrono>

using model::broker;
using model::node_id;
using std::vector;

namespace cluster {

namespace {

void verify_duplicate_seed_servers() {
    std::vector<config::seed_server> s = config::node().seed_servers();
    std::sort(s.begin(), s.end());
    const auto s_dupe_i = std::adjacent_find(s.cbegin(), s.cend());
    if (s_dupe_i != s.cend())
        throw std::runtime_error(fmt_with_ctx(
          fmt::format, "Duplicate items in seed_servers: {}", *s_dupe_i));
}

} // namespace

cluster_discovery::cluster_discovery(
  const model::node_uuid& node_uuid,
  storage::kvstore& kvstore,
  ss::abort_source& as)
  : _node_uuid(node_uuid)
  , _join_retry_jitter(config::shard_local_cfg().join_retry_timeout_ms())
  , _join_timeout(std::chrono::seconds(2))
  , _kvstore(kvstore)
  , _as(as) {
    verify_duplicate_seed_servers();
}

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

ss::future<cluster_discovery::brokers>
cluster_discovery::initial_seed_brokers(const bool cluster_exists) const {
    // If the cluster has been formed, return empty
    if (cluster_exists) co_return brokers{};
    // If configured as the root node, we'll want to start the cluster with
    // just this node as the initial seed.
    if (config::node().empty_seed_starts_cluster()) {
        if (config::node().seed_servers().empty())
            co_return brokers{make_self_broker(config::node())};
        // Not a root
        co_return brokers{};
    }
    if (get_node_index_in_seed_servers()) {
        std::vector<model::broker> seed_brokers
          = co_await request_seed_brokers();
        seed_brokers.push_back(make_self_broker(config::node()));
        co_return std::move(seed_brokers);
    }
    // Non-seed server
    co_return brokers{};
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
        auto r = co_await do_with_client_one_shot<controller_client_protocol>(
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
        if (!r || r.has_error() || !r.value().success) {
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

ss::future<cluster_bootstrap_info_reply>
cluster_discovery::request_cluster_bootstrap_info(
  const net::unresolved_address addr) const {
    vlog(clusterlog.info, "Requesting cluster bootstrap info from {}", addr);
    cluster_bootstrap_info_reply reply;
    co_await ss::repeat(
      ss::coroutine::lambda([&reply, addr]() -> ss::future<ss::stop_iteration> {
          auto reply_result = co_await do_with_client_one_shot<
            cluster_bootstrap_client_protocol>(
            addr,
            config::node().rpc_server_tls(),
            2s,
            [](cluster_bootstrap_client_protocol c) {
                return c
                  .cluster_bootstrap_info(
                    cluster_bootstrap_info_request{},
                    rpc::client_opts(rpc::clock_type::now() + 2s))
                  .then(&rpc::get_ctx_data<cluster_bootstrap_info_reply>);
            });
          if (reply_result) {
              reply = std::move(reply_result.value());
              co_return ss::stop_iteration::yes;
          }
          co_await ss::sleep_abortable(1s);
          vlog(
            clusterlog.trace, "Retrying cluster bootstrap info from {}", addr);
          co_return ss::stop_iteration::no;
      }));

    vlog(clusterlog.info, "Obtained cluster bootstrap info from {}", addr);
    vlog(clusterlog.debug, "{}", reply);
    co_return std::move(reply);
}

namespace {

bool equal(
  const std::vector<net::unresolved_address>& lhs,
  const std::vector<config::seed_server>& rhs) {
    return std::equal(
      lhs.cbegin(),
      lhs.cend(),
      rhs.cbegin(),
      rhs.cend(),
      [](const net::unresolved_address& lhs, const config::seed_server& rhs) {
          return lhs == rhs.addr;
      });
}

} // namespace

ss::future<std::vector<model::broker>>
cluster_discovery::request_seed_brokers() const {
    const net::unresolved_address& self_addr
      = config::node().advertised_rpc_api();
    const std::vector<config::seed_server>& self_seed_servers
      = config::node().seed_servers();

    std::vector<
      std::pair<net::unresolved_address, cluster_bootstrap_info_reply>>
      peers;
    peers.reserve(self_seed_servers.size());
    for (const config::seed_server& seed_server : self_seed_servers) {
        // do not call oneself
        if (seed_server.addr == self_addr) continue;
        peers.emplace_back(seed_server.addr, cluster_bootstrap_info_reply{});
    }
    co_await ss::parallel_for_each(peers, [this](auto& peer) -> ss::future<> {
        peer.second = co_await request_cluster_bootstrap_info(peer.first);
        co_return;
    });

    bool failed = false;
    std::vector<model::broker> seed_brokers;
    seed_brokers.reserve(peers.size());
    for (auto& peer : peers) {
        if (
          peer.second.version
          != features::feature_table::get_latest_logical_version()) {
            vlog(
              clusterlog.error,
              "Cluster setup error: logical version mismatch, local: {}, {}: "
              "{}",
              features::feature_table::get_latest_logical_version(),
              peer.first,
              peer.second.version);
            failed = true;
        }
        if (!equal(peer.second.seed_servers, self_seed_servers)) {
            vlog(
              clusterlog.error,
              "Cluster configuration error: seed server list mismatch, "
              "local: "
              "[{}], {}: [{}]",
              self_seed_servers,
              peer.first,
              peer.second.seed_servers);
            failed = true;
        }
        if (
          peer.second.empty_seed_starts_cluster
          != config::node().empty_seed_starts_cluster()) {
            vlog(
              clusterlog.error,
              "Cluster configuration error: empty_seed_starts_cluster "
              "mismatch, local: {}, {}: {}",
              config::node().empty_seed_starts_cluster(),
              peer.first,
              peer.second.empty_seed_starts_cluster);
            failed = true;
        }
        seed_brokers.push_back(std::move(peer.second.broker));
    }
    if (failed)
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Cannot bootstrap a cluster due to seed servers mismatch, check "
          "the "
          "log for details"));
    vlog(clusterlog.debug, "Seed brokers: [{}]", seed_brokers);
    co_return std::move(seed_brokers);
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
