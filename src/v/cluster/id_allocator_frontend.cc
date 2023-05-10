// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/id_allocator_frontend.h"

#include "cluster/controller.h"
#include "cluster/id_allocator_service.h"
#include "cluster/id_allocator_stm.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/metadata_cache.h"
#include "cluster/partition_leaders_table.h"
#include "cluster/partition_manager.h"
#include "cluster/shard_table.h"
#include "cluster/topics_frontend.h"
#include "cluster/tx_helpers.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "rpc/connection_cache.h"
#include "vformat.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sharded.hh>

namespace cluster {
using namespace std::chrono_literals;

cluster::errc map_errc_fixme(std::error_code ec);

class id_allocator_frontend;

allocate_id_router::allocate_id_router(
  id_allocator_frontend& frontend,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : leader_routing_frontend<
    allocate_id_request,
    allocate_id_reply,
    id_allocator_client_protocol>(
    shard_table,
    metadata_cache,
    connection_cache,
    leaders,
    model::id_allocator_nt,
    node_id,
    config::shard_local_cfg().metadata_dissemination_retries.value(),
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _frontend(frontend) {}

id_reset_router::id_reset_router(
  id_allocator_frontend& frontend,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id)
  : leader_routing_frontend<
    reset_id_allocator_request,
    reset_id_allocator_reply,
    id_allocator_client_protocol>(
    shard_table,
    metadata_cache,
    connection_cache,
    leaders,
    model::id_allocator_nt,
    node_id,
    config::shard_local_cfg().metadata_dissemination_retries.value(),
    config::shard_local_cfg().metadata_dissemination_retry_delay_ms.value())
  , _frontend(frontend) {}

ss::future<result<rpc::client_context<allocate_id_reply>>>
allocate_id_router::dispatch(
  id_allocator_client_protocol proto,
  allocate_id_request req,
  model::timeout_clock::duration timeout) {
    return proto.allocate_id(
      std::move(req), rpc::client_opts(model::timeout_clock::now() + timeout));
}

ss::future<allocate_id_reply>
allocate_id_router::process(ss::shard_id shard, allocate_id_request req) {
    auto res = co_await _frontend.run_on_shard(
      shard, [timeout = req.timeout](id_allocator_stm& stm) {
          return stm.allocate_id(timeout);
      });
    if (res.raft_status == raft::errc::success) {
        co_return allocate_id_reply{res.id, errc::success};
    }
    if (res.raft_status == raft::errc::group_not_exists) {
        co_return allocate_id_reply{0, errc::topic_not_exists};
    }
    co_return allocate_id_reply{0, errc::replication_error};
}

ss::future<reset_id_allocator_reply>
id_reset_router::process(ss::shard_id shard, reset_id_allocator_request req) {
    auto res = co_await _frontend.run_on_shard(
      shard,
      [timeout = req.timeout, id = req.producer_id](id_allocator_stm& stm) {
          return stm.reset_next_id(id, timeout);
      });
    if (res.raft_status == raft::errc::success) {
        co_return reset_id_allocator_reply{errc::success};
    }
    if (res.raft_status == raft::errc::group_not_exists) {
        co_return reset_id_allocator_reply{errc::topic_not_exists};
    }
    co_return reset_id_allocator_reply{errc::replication_error};
}

id_allocator_frontend::id_allocator_frontend(
  ss::smp_service_group ssg,
  ss::sharded<cluster::partition_manager>& partition_manager,
  ss::sharded<cluster::shard_table>& shard_table,
  ss::sharded<cluster::metadata_cache>& metadata_cache,
  ss::sharded<rpc::connection_cache>& connection_cache,
  ss::sharded<partition_leaders_table>& leaders,
  const model::node_id node_id,
  std::unique_ptr<cluster::controller>& controller)
  : _ssg(ssg)
  , _partition_manager(partition_manager)
  , _metadata_cache(metadata_cache)
  , _controller(controller)
  , _allocator_router(
      *this, shard_table, metadata_cache, connection_cache, leaders, node_id)
  , _reset_router(
      *this, shard_table, metadata_cache, connection_cache, leaders, node_id) {}

ss::future<allocate_id_reply>
id_allocator_frontend::allocate_id(model::timeout_clock::duration timeout) {
    auto nt = model::topic_namespace(
      model::kafka_internal_namespace, model::id_allocator_topic);

    auto has_topic = true;

    if (!_metadata_cache.local().contains(nt, model::partition_id(0))) {
        has_topic = co_await try_create_id_allocator_topic();
    }

    if (!has_topic) {
        vlog(clusterlog.warn, "can't find {} in the metadata cache", nt);
        co_return allocate_id_reply{0, errc::topic_not_exists};
    }

    co_return co_await _allocator_router.process_or_dispatch(
      allocate_id_request{timeout}, model::partition_id(0), timeout);
}

template<typename id_allocator_func>
ss::future<id_allocator_stm::stm_allocation_result>
id_allocator_frontend::run_on_shard(
  ss::shard_id shard, id_allocator_func func) {
    using result_t = id_allocator_stm::stm_allocation_result;
    return _partition_manager.invoke_on(
      shard, _ssg, [func](cluster::partition_manager& mgr) mutable {
          auto partition = mgr.get(model::id_allocator_ntp);
          if (!partition) {
              vlog(
                clusterlog.warn,
                "can't get partition by {} ntp",
                model::id_allocator_ntp);
              return ss::make_ready_future<result_t>(
                result_t{0, raft::errc::group_not_exists});
          }
          auto stm = partition->id_allocator_stm();
          if (!stm) {
              vlog(
                clusterlog.warn,
                "can't get id allocator stm of the {}' partition",
                model::id_allocator_ntp);
              return ss::make_ready_future<result_t>(
                result_t{0, raft::errc::group_not_exists});
          }
          return func(*stm).then([](id_allocator_stm::stm_allocation_result r) {
              if (r.raft_status != raft::errc::success) {
                  vlog(
                    clusterlog.warn,
                    "id allocator stm call failed with {}",
                    raft::make_error_code(r.raft_status).message());
              }
              return r;
          });
      });
}

ss::future<bool> id_allocator_frontend::try_create_id_allocator_topic() {
    cluster::topic_configuration topic{
      model::kafka_internal_namespace,
      model::id_allocator_topic,
      1,
      _controller->internal_topic_replication()};

    topic.properties.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::none;

    return _controller->get_topics_frontend()
      .local()
      .autocreate_topics(
        {std::move(topic)}, config::shard_local_cfg().create_topic_timeout_ms())
      .then([](std::vector<cluster::topic_result> res) {
          vassert(res.size() == 1, "expected exactly one result");
          if (res[0].ec != cluster::errc::success) {
              vlog(
                clusterlog.warn,
                "can not create {}/{} topic - error: {}",
                model::kafka_internal_namespace,
                model::id_allocator_topic,
                cluster::make_error_code(res[0].ec).message());
              return false;
          }
          return true;
      })
      .handle_exception([](std::exception_ptr e) {
          vlog(
            clusterlog.warn,
            "can not create {}/{} topic - error: {}",
            model::kafka_internal_namespace,
            model::id_allocator_topic,
            e);
          return false;
      });
}

} // namespace cluster
