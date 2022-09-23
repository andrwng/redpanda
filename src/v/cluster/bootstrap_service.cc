// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/bootstrap_service.h"

#include "cluster/bootstrap_types.h"
#include "storage/api.h"

namespace cluster {

ss::future<get_node_uuid_reply> bootstrap_service::get_node_uuid(
  get_node_uuid_request&&, rpc::streaming_context&) {
    get_node_uuid_reply r;
    r.node_uuid = _storage.local().node_uuid();
    co_return r;
}

} // namespace cluster
