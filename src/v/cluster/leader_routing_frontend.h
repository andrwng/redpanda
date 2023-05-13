// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/shard_table.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "rpc/connection_cache.h"
#include "seastarx.h"
#include "vformat.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

namespace rpc {
class connection_cache;
} // namespace rpc

namespace cluster {

// Encapsulates a frontend that aims to direct RPCs to a specific NTP's leader.
//
// When a client invokes process_or_dispatch() on a remote node, the frontend
// redirects the request to the service located on the broker that hosts the
// NTP's leader.
//
// When a client is colocated with the leader, the frontend calls
// find_shard_and_process() directly, calling into subclasses' process()
// implementations.
template<typename req_t, typename resp_t, typename proto_t>
class leader_routing_frontend {
public:
    using duration = model::timeout_clock::duration;
    leader_routing_frontend(
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      model::topic_namespace,
      model::node_id self,
      int16_t,
      std::chrono::milliseconds);

    // Sends the given request to the leader, or processes it on the
    // appropriate shard if this node is the leader.
    ss::future<resp_t>
    process_or_dispatch(req_t req, model::partition_id, duration timeout);

    // Finds the shard appropriate for the NTP and processes the given request,
    // expecting the leader is on this node.
    ss::future<resp_t>
    find_shard_and_process(req_t req, model::partition_id, duration timeout);

    ss::future<> shutdown() {
        _as.request_abort();
        co_await _gate.close();
    }

private:
    // Sends the given request on the given client protocol with the given
    // timeout.
    virtual ss::future<result<rpc::client_context<resp_t>>>
    dispatch(proto_t proto, req_t req, duration timeout) = 0;

    // Processes the given request on the given shard. This is expected to run
    // on the leader.
    virtual ss::future<resp_t> process(ss::shard_id, req_t req) = 0;

    // Returns an error response with the given code.
    virtual resp_t error_resp(cluster::errc) const = 0;

    // Returns a short description of the process being dispatched to leader,
    // suitable for logging.
    virtual ss::sstring process_name() const = 0;

    // Sends the given request to the given leader node ID..
    virtual ss::future<resp_t>
    dispatch_to_leader(req_t req, model::node_id, duration timeout);

    ss::abort_source _as;
    ss::gate _gate;
    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_leaders_table>& _leaders;
    const model::topic_namespace _topic;
    const model::node_id _self;

    int16_t _retries{1};
    std::chrono::milliseconds _retry_delay_ms;
};

template<typename req_t, typename resp_t, typename proto_t>
leader_routing_frontend<req_t, resp_t, proto_t>::leader_routing_frontend(
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  model::topic_namespace topic,
  model::node_id self,
  int16_t retries,
  std::chrono::milliseconds retry_delay_ms)
  : _shard_table(shard_table)
  , _metadata_cache(metadata_cache)
  , _connection_cache(connection_cache)
  , _leaders(leaders)
  , _topic(std::move(topic))
  , _self(self)
  , _retries(retries)
  , _retry_delay_ms(retry_delay_ms) {}

template<typename req_t, typename resp_t, typename proto_t>
ss::future<resp_t>
leader_routing_frontend<req_t, resp_t, proto_t>::process_or_dispatch(
  req_t req,
  model::partition_id partition_id,
  model::timeout_clock::duration timeout) {
    auto retries = _retries;
    auto delay_ms = _retry_delay_ms;
    std::optional<std::string> error;

    auto holder = ss::gate::holder(_gate);
    auto r = error_resp(errc::not_leader);
    const auto ntp = model::ntp(_topic.ns, _topic.tp, partition_id);
    while (!_as.abort_requested() && 0 < retries--) {
        auto leader_opt = _leaders.local().get_leader(ntp);
        if (unlikely(!leader_opt)) {
            error = vformat(
              fmt::runtime("can't find {} in the leaders cache"), ntp);
            vlog(
              clusterlog.trace,
              "waiting for {} to fill leaders cache, retries left: {}",
              ntp,
              retries);
            co_await sleep_abortable(delay_ms, _as);
            continue;
        }
        auto leader = leader_opt.value();

        if (leader == _self) {
            r = co_await find_shard_and_process(req, partition_id, timeout);
        } else {
            vlog(
              clusterlog.trace,
              "dispatching {} from {} to {} ",
              process_name(),
              _self,
              leader);
            r = co_await dispatch_to_leader(req, leader, timeout);
        }

        if (likely(r.ec == errc::success)) {
            error = std::nullopt;
            break;
        }

        if (likely(r.ec != errc::replication_error)) {
            error = vformat(
              fmt::runtime("{} failed with {}"), process_name(), r.ec);
            break;
        }

        error = vformat(
          fmt::runtime("{} failed with {}"), process_name(), r.ec);
        vlog(
          clusterlog.trace,
          "{} failed, retries left: {}",
          process_name(),
          retries);
        co_await sleep_abortable(delay_ms, _as);
    }
    if (error) {
        vlog(clusterlog.warn, "{} failed: {}", process_name(), error.value());
    }

    co_return r;
}

template<typename req_t, typename resp_t, typename proto_t>
ss::future<resp_t>
leader_routing_frontend<req_t, resp_t, proto_t>::dispatch_to_leader(
  req_t req, model::node_id leader_id, model::timeout_clock::duration timeout) {
    return _connection_cache.local()
      .with_node_client<proto_t>(
        _self,
        ss::this_shard_id(),
        leader_id,
        timeout,
        [this, req = std::move(req), timeout](proto_t proto) {
            return dispatch(std::move(proto), std::move(req), timeout);
        })
      .then(&rpc::get_ctx_data<resp_t>)
      .then([this](result<resp_t> r) {
          if (r.has_error()) {
              vlog(
                clusterlog.warn,
                "got error {} on remote {}",
                r.error().message(),
                process_name());
              return error_resp(errc::timeout);
          }
          return r.value();
      });
}

template<typename req_t, typename resp_t, typename proto_t>
ss::future<resp_t>
leader_routing_frontend<req_t, resp_t, proto_t>::find_shard_and_process(
  req_t req,
  model::partition_id partition_id,
  model::timeout_clock::duration timeout) {
    const auto ntp = model::ntp(_topic.ns, _topic.tp, partition_id);
    auto shard = _shard_table.local().shard_for(ntp);

    auto holder = ss::gate::holder(_gate);
    if (unlikely(!shard)) {
        auto retries = _retries;
        auto delay_ms = _retry_delay_ms;
        while (!_as.abort_requested() && !shard && 0 < retries--) {
            try {
                co_await sleep_abortable(delay_ms, _as);
            } catch (ss::sleep_aborted& e) {
                break;
            }
            shard = _shard_table.local().shard_for(ntp);
        }

        if (!shard) {
            vlog(clusterlog.warn, "can't find {} in the shard table", ntp);
            co_return error_resp(errc::no_leader_controller);
        }
    }
    co_return co_await process(*shard, std::move(req));
}

} // namespace cluster
