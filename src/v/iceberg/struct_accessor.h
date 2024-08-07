// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg {

// Nested accessor to get child values from a struct value, e.g. to build a
// partition key for a struct value.
//
// NOTE: intended use is just for building the partition key, and therefore no
// support for searching lists or maps is done here -- just structs and
// primitives. Other partitioning values are not supported by the Iceberg spec.
class struct_accessor {
public:
    using ids_accessor_map_t
      = chunked_hash_map<nested_field::id_t, std::unique_ptr<struct_accessor>>;

    static std::unique_ptr<struct_accessor>
    create(size_t position, const primitive_type& type) {
        return std::make_unique<struct_accessor>(position, type);
    }
    static std::unique_ptr<struct_accessor>
    create(size_t position, std::unique_ptr<struct_accessor> inner) {
        return std::make_unique<struct_accessor>(position, std::move(inner));
    }
    static ids_accessor_map_t from_struct_type(const struct_type&);

    std::optional<const std::reference_wrapper<value>>
    get(const struct_value& parent_val) const;

    // Public for make_unique<> only. Use struct_accessor::create() instead!
    struct_accessor(size_t position, const primitive_type& type)
      : position_(position)
      , type_(type) {}
    struct_accessor(size_t position, std::unique_ptr<struct_accessor> inner)
      : position_(position)
      , type_(inner->type_)
      , inner_(std::move(inner)) {}

private:
    // The position that this accessor will operate on when calling get().
    size_t position_;

    // If `inner_` is set, `position_` is expected to refer to a struct field
    // by callers of get(). Otherwise, the value referred to by `position_`
    // is expected to be a value of type `type_`.
    primitive_type type_;
    std::unique_ptr<struct_accessor> inner_;
};

} // namespace iceberg
