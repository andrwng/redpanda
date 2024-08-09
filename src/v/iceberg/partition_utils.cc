// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/partition_utils.h"

#include "iceberg/transform_utils.h"

namespace iceberg {

struct_type create_partition_type(
  const partition_spec& partition_spec, const schema& schema) {
    struct_type schema_struct;
    const auto ids_to_types = schema.ids_to_types();
    const auto& partition_fields = partition_spec.fields;
    for (const auto& field : partition_fields) {
        const auto& source_id = field.source_id;
        const auto type_iter = ids_to_types.find(source_id);
        if (type_iter == ids_to_types.end()) {
            throw std::invalid_argument(fmt::format(
              "Expected source field ID {} to be in schema", source_id()));
        }
        const auto& source_type = *type_iter->second;
        auto result_field = nested_field::create(
          field.field_id,
          field.name,
          field_required::yes,
          get_result_type(source_type, field.transform));
        schema_struct.fields.emplace_back(std::move(result_field));
    }
    return schema_struct;
}

} // namespace iceberg
