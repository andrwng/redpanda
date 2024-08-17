// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "iceberg/avro_utils.h"
#include "iceberg/manifest_entry.avrogen.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_entry_avro.h"
#include "iceberg/partition.h"
#include "iceberg/partition_key_type.h"
#include "iceberg/schema.h"
#include "iceberg/tests/test_schemas.h"
#include "iceberg/values_avro.h"
#include "utils/file_io.h"

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
    auto partition_type = std::move(
      partition_key_type::create(test_spec, source_schema).type);

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

TEST(ManifestEntryAvroTest, TestPartitionFieldManifestEntry) {
    // Write out the manifest entry as an iobuf, and try reading it with
    // code-generated code.
    data_file f{
      .content_type = data_file_content_type::equality_deletes,
      .file_path = "/foo/bar/baz",
      .file_format = data_file_format::parquet,
      // Use an empty partition type, since avrogen doesn't support the
      // dynamic partition key.
      .partition = partition_key{std::make_unique<struct_value>()},
      .record_count = 3,
      .file_size_bytes = 1024,
      .column_sizes = {},
      .value_counts = {},
      .null_value_counts = {},
      .nan_value_counts = {},
    };
    manifest_entry e{
      .status = manifest_entry_status::added,
      .snapshot_id = std::nullopt,
      .sequence_number = std::nullopt,
      .file_sequence_number = std::nullopt,
      .data_file = std::move(f),
    };
    auto manifest_entry_val = manifest_entry_to_value(std::move(e));
    auto buf = struct_to_avro_buf(
      manifest_entry_val, manifest_entry_type(struct_type{}), "manifest_entry");

    auto in = std::make_unique<avro_iobuf_istream>(buf.copy());
    avro::DataFileReader<avrogen::manifest_entry> reader(
      std::move(in), avrogen::manifest_entry::valid_schema());

    avrogen::manifest_entry avro_entry;
    reader.read(avro_entry);
    ASSERT_EQ(avro_entry.status, 1);
    ASSERT_TRUE(avro_entry.snapshot_id.is_null());
    ASSERT_TRUE(avro_entry.sequence_number.is_null());
    ASSERT_TRUE(avro_entry.file_sequence_number.is_null());
    ASSERT_EQ(avro_entry.data_file.content, 2);
    ASSERT_STREQ(avro_entry.data_file.file_path.c_str(), "/foo/bar/baz");
    ASSERT_STREQ(avro_entry.data_file.file_format.c_str(), "parquet");
    ASSERT_EQ(avro_entry.data_file.record_count, 3);
    ASSERT_EQ(avro_entry.data_file.file_size_in_bytes, 1024);
}
