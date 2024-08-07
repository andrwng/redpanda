// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/struct_accessor.h"

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

#include <variant>

namespace iceberg {

struct_accessor::ids_accessor_map_t
struct_accessor::from_struct_type(const struct_type& s) {
    ids_accessor_map_t ret;
    for (size_t i = 0; i < s.fields.size(); i++) {
        const auto& field = s.fields[i];
        if (!field) {
            continue;
        }
        const auto& child = field->type;
        if (std::holds_alternative<primitive_type>(child)) {
            ret.emplace(
              field->id,
              struct_accessor::create(i, std::get<primitive_type>(child)));
        } else if (std::holds_alternative<struct_type>(child)) {
            const auto& child_as_struct = std::get<struct_type>(child);
            auto child_accessors = from_struct_type(child_as_struct);
            for (auto& [id, child_accessor] : child_accessors) {
                ret.emplace(
                  id, struct_accessor::create(i, std::move(child_accessor)));
            }
        }
    }
    return ret;
}

std::optional<const std::reference_wrapper<value>>
struct_accessor::get(const struct_value& parent_val) const {
    if (position_ >= parent_val.fields.size()) {
        throw std::invalid_argument(fmt::format(
          "Invalid access to position {}, struct has {} fields",
          position_,
          parent_val.fields.size()));
    }
    const auto& child_val = parent_val.fields[position_];
    if (!child_val) {
        return std::nullopt;
    }
    if (inner_) {
        const auto& child_val = parent_val.fields[position_];
        if (!std::holds_alternative<struct_value>(*child_val)) {
            throw std::invalid_argument("Unexpected non-struct value");
        }
        const auto& child_as_struct = std::get<struct_value>(*child_val);
        return inner_->get(child_as_struct);
    }
    if (!std::holds_alternative<primitive_value>(
          *parent_val.fields[position_])) {
        throw std::invalid_argument("Unexpected non-primitive value");
    }
    return *child_val;
}

} // namespace iceberg
