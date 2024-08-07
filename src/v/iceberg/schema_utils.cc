// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/schema_utils.h"

namespace iceberg {

ids_types_map_t ids_to_types(const schema& s) {
    chunked_vector<const nested_field*> to_visit;
    for (const auto& field : s.schema_struct.fields) {
        to_visit.emplace_back(field.get());
    }
    ids_types_map_t ret;
    while (!to_visit.empty()) {
        auto* field = to_visit.back();
        to_visit.pop_back();
        if (!field) {
            continue;
        }
        const auto& type = field->type;
        ret.emplace(field->id, &type);
        if (std::holds_alternative<list_type>(type)) {
            const auto& type_as_list = std::get<list_type>(type);
            to_visit.emplace_back(type_as_list.element_field.get());
        } else if (std::holds_alternative<struct_type>(type)) {
            const auto& type_as_struct = std::get<struct_type>(type);
            for (const auto& field : type_as_struct.fields) {
                to_visit.emplace_back(field.get());
            }
        } else if (std::holds_alternative<map_type>(type)) {
            const auto& type_as_map = std::get<map_type>(type);
            to_visit.emplace_back(type_as_map.key_field.get());
            to_visit.emplace_back(type_as_map.value_field.get());
        }
    }
    return ret;
}

} // namespace iceberg
