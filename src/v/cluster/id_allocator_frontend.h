/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/id_allocator_service.h"
#include "cluster/leader_routing_frontend.h"
#include "cluster/types.h"
#include "rpc/fwd.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>

#include <vector>

namespace cluster {

class id_allocator;

class allocate_id_router
  : public leader_routing_frontend<
      allocate_id_request,
      allocate_id_reply,
      id_allocator_client_protocol> {
public:
    allocate_id_router(
      id_allocator_frontend&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id);
    ~allocate_id_router() = default;

    ss::future<result<rpc::client_context<allocate_id_reply>>> dispatch(
      id_allocator_client_protocol proto,
      allocate_id_request,
      model::timeout_clock::duration timeout) override;

    ss::future<allocate_id_reply>
    process(ss::shard_id, allocate_id_request req) override;

    allocate_id_reply error_resp(cluster::errc e) const override {
        return allocate_id_reply{0, e};
    }

    ss::sstring process_name() const override { return "id allocation"; }

private:
    id_allocator_frontend& _frontend;
};

// id_allocator_frontend is an frontend of the id_allocator_stm,
// an engine behind the id_allocator service.
//
// when a client invokes id_allocator_frontend::allocate_id on a
// remote node the frontend redirects the request to the service
// located on the leading broker of the id_allocator's partition.
//
// when a client is located on the same node as the leader then the
// frontend bypasses the network and directly engages with the state
// machine (id_allocator_stm.cc)
//
// when the service recieves a call it triggers id_allocator_frontend
// which in its own turn pass the request to the id_allocator_stm
class id_allocator_frontend {
public:
    id_allocator_frontend(
      ss::smp_service_group,
      ss::sharded<cluster::partition_manager>&,
      ss::sharded<cluster::shard_table>&,
      ss::sharded<cluster::metadata_cache>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_leaders_table>&,
      const model::node_id,
      std::unique_ptr<cluster::controller>&);

    ss::future<allocate_id_reply>
    allocate_id(model::timeout_clock::duration timeout);

    ss::future<> stop() { return _allocator_router.shutdown(); }

    allocate_id_router& allocator_router() { return _allocator_router; }

    template<typename id_allocator_func>
    ss::future<allocate_id_reply>
    run_on_shard(ss::shard_id, id_allocator_func func);

private:
    ss::smp_service_group _ssg;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::metadata_cache>& _metadata_cache;
    std::unique_ptr<cluster::controller>& _controller;

    allocate_id_router _allocator_router;

    // Sets the underlying stm's next id to the given id, returning an error if
    // there was a problem (e.g. not leader, timed out, etc).
    ss::future<allocate_id_reply>
      do_reset_next_id(int64_t, model::timeout_clock::duration);

    ss::future<bool> try_create_id_allocator_topic();

    friend id_allocator;
};
} // namespace cluster
