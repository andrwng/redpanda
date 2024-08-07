// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/chunked_hash_map.h"
#include "iceberg/datatypes.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

namespace iceberg {

struct schema {
    using id_t = named_type<int32_t, struct schema_id_tag>;
    using ids_types_map_t
      = chunked_hash_map<nested_field::id_t, const field_type*>;

    struct_type schema_struct;
    id_t schema_id;
    chunked_hash_set<nested_field::id_t> identifier_field_ids;
    friend bool operator==(const schema& lhs, const schema& rhs) = default;

    // Returns a mapping from field id to the field type.
    ids_types_map_t ids_to_types() const;
};

} // namespace iceberg
