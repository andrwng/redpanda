// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema.h"

namespace iceberg {

namespace {
struct nested_field_collecting_visitor {
public:
    explicit nested_field_collecting_visitor(
      chunked_vector<const nested_field*>& to_visit)
      : to_visit_(to_visit) {}
    chunked_vector<const nested_field*>& to_visit_;

    void operator()(const primitive_type&) {
        // No-op, no additional fields to collect.
    }
    void operator()(const list_type& t) {
        to_visit_.emplace_back(t.element_field.get());
    }
    void operator()(const struct_type& t) {
        for (const auto& field : t.fields) {
            to_visit_.emplace_back(field.get());
        }
    }
    void operator()(const map_type& t) {
        to_visit_.emplace_back(t.key_field.get());
        to_visit_.emplace_back(t.value_field.get());
    }
};
} // namespace

schema::ids_types_map_t schema::ids_to_types() const {
    chunked_vector<const nested_field*> to_visit;
    for (const auto& field : schema_struct.fields) {
        to_visit.emplace_back(field.get());
    }
    schema::ids_types_map_t ret;
    while (!to_visit.empty()) {
        auto* field = to_visit.back();
        if (!field) {
            // E.g. empty value field.
            continue;
        }
        const auto& type = field->type;
        to_visit.pop_back();
        ret.emplace(field->id, &type);
        std::visit(nested_field_collecting_visitor{to_visit}, type);
    }
    return ret;
}

} // namespace iceberg
