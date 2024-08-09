// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/manifest_entry_avro.h"
#include "iceberg/partition.h"
#include "iceberg/partition_utils.h"
#include "iceberg/schema.h"
#include "iceberg/tests/test_schemas.h"

#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <gtest/gtest.h>

using namespace iceberg;

namespace {
// Partition field where the field 0 gets the hour_transform applied to it.
const partition_field test_partition_field{
  .source_id = nested_field::id_t{0},
  .field_id = partition_field::id_t{1000},
  .name = "test_partition",
  .transform = iceberg::hour_transform(),
};
nested_field_ptr make_timestamp_field() {
    return nested_field::create(
      nested_field::id_t{0},
      "test_timestamp",
      field_required::yes,
      timestamp_type());
}
partition_spec make_timestamp_partition_spec() {
    return partition_spec{partition_spec::id_t{0}, {test_partition_field}};
}

} // namespace

TEST(ManifestEntrySchemaTest, TestEmptyPartitionFieldValidSchema) {
    auto partition_type = struct_type();
    auto schema = avro::ValidSchema(manifest_entry_schema(partition_type));
    auto expected_partition_field = R"(
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": []
                        },
                        "field-id": 102
                    }
)";
    const auto schema_json = schema.toJson(true);
    ASSERT_TRUE(schema_json.find(expected_partition_field) != ss::sstring::npos)
      << schema_json;
}

TEST(ManifestEntrySchemaTest, TestPartitionFieldValidSchema) {
    auto test_struct = test_nested_schema_type();
    std::get<struct_type>(test_struct)
      .fields.emplace_back(make_timestamp_field());
    const schema source_schema{
      .schema_struct = std::move(std::get<struct_type>(test_struct)),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    const auto test_spec = make_timestamp_partition_spec();
    auto partition_type = create_partition_type(test_spec, source_schema);

    // Check that the Avro schema generated for the manifest entry looks
    // correct.
    auto schema = avro::ValidSchema(manifest_entry_schema(partition_type));
    auto expected_partition_field = R"(
                    {
                        "name": "partition",
                        "type": {
                            "type": "record",
                            "name": "r102",
                            "fields": [
                                {
                                    "name": "test_partition",
                                    "type": "int",
                                    "field-id": 1000
                                }
                            ]
                        },
                        "field-id": 102
                    }
)";
    const auto schema_json = schema.toJson(true);
    ASSERT_TRUE(schema_json.find(expected_partition_field) != ss::sstring::npos)
      << schema_json;
}

TEST(ManifestEntryAvroTest, TestPartitionFieldManifestEntry) {}
