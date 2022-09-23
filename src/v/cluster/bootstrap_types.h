// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "serde/serde.h"

#include <fmt/core.h>

#include <vector>

namespace cluster {

struct get_node_uuid_request
  : serde::envelope<get_node_uuid_request, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;

    friend std::ostream&
    operator<<(std::ostream& o, const get_node_uuid_request&) {
        fmt::print(o, "{{}}");
        return o;
    }

    auto serde_fields() { return std::tie(); }
};

struct get_node_uuid_reply
  : serde::envelope<get_node_uuid_reply, serde::version<0>> {
    using rpc_adl_exempt = std::true_type;

    std::vector<uint8_t> node_uuid;

    auto serde_fields() { return std::tie(node_uuid); }

    friend std::ostream&
    operator<<(std::ostream& o, const get_node_uuid_reply& rep) {
        fmt::print(o, "{{ node_uuid: {} }}", rep.node_uuid);
        return o;
    }
};

} // namespace cluster
