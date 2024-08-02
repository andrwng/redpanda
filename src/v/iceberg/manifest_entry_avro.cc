// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_entry_avro.h"

#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>
#include <fmt/core.h>

namespace iceberg {

avro::CustomAttributes field_attrs(int field_id) {
    avro::CustomAttributes attrs;
    attrs.addAttribute("field-id", fmt::format("{}", field_id));
    return attrs;
}

avro::Schema optional_of(const avro::Schema& type) {
    avro::UnionSchema s;
    s.addType(avro::NullSchema());
    s.addType(type);
    return s;
}

avro::Schema map_of(const avro::Schema& kv_type) {
    // Map types are represented as an array of key-value pairs with a logical
    // type of "map".
    avro::ArraySchema schema(kv_type);
    schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::MAP));
    return schema;
}

avro::Schema array_of(const avro::Schema& type, int element_id) {
    avro::ArraySchema schema(type, element_id);
    return schema;
}

avro::Schema kv_schema(
  const std::string& name,
  int key_id,
  const avro::Schema& key_schema,
  int val_id,
  const avro::Schema& val_schema) {
    avro::RecordSchema schema(name);
    schema.addField("key", key_schema, field_attrs(key_id));
    schema.addField("value", val_schema, field_attrs(val_id));
    return schema;
}

avro::Schema partition_schema() {
    // TODO: pass in a tuple.
    avro::RecordSchema schema("r102");
    return schema;
}

avro::Schema data_file_schema() {
    avro::RecordSchema schema("r2");
    schema.addField("conent", avro::IntSchema(), field_attrs(134));
    schema.addField("file_path", avro::IntSchema(), field_attrs(100));
    schema.addField("file_format", avro::StringSchema(), field_attrs(101));
    schema.addField("partition", partition_schema(), field_attrs(102));
    schema.addField("record_count", avro::LongSchema(), field_attrs(103));
    schema.addField("file_size_in_bytes", avro::LongSchema(), field_attrs(104));
    schema.addField(
      "column_sizes",
      optional_of(map_of(kv_schema(
        "k117_v118", 117, avro::IntSchema(), 118, avro::LongSchema()))),
      field_attrs(108));
    schema.addField(
      "value_counts",
      optional_of(map_of(kv_schema(
        "k119_v120", 119, avro::IntSchema(), 119, avro::LongSchema()))),
      field_attrs(109));
    schema.addField(
      "null_value_counts",
      optional_of(map_of(kv_schema(
        "k121_v122", 121, avro::IntSchema(), 122, avro::LongSchema()))),
      field_attrs(110));
    schema.addField(
      "nan_value_counts",
      optional_of(map_of(kv_schema(
        "k138_v139", 138, avro::IntSchema(), 139, avro::LongSchema()))),
      field_attrs(137));
    // NOTE: distinct_counts(111) left out, since it doesn't seem widely
    // adopted across implementations.
    schema.addField(
      "lower_bounds",
      optional_of(map_of(kv_schema(
        "k126_v127", 126, avro::IntSchema(), 127, avro::BytesSchema()))),
      field_attrs(125));
    schema.addField(
      "upper_bounds",
      optional_of(map_of(kv_schema(
        "k129_v130", 129, avro::IntSchema(), 130, avro::BytesSchema()))),
      field_attrs(128));
    schema.addField(
      "key_metadata", optional_of(avro::BytesSchema()), field_attrs(131));
    schema.addField(
      "split_offsets",
      optional_of(array_of(avro::LongSchema(), 133)),
      field_attrs(132));
    schema.addField(
      "equality_ids",
      optional_of(array_of(avro::IntSchema(), 136)),
      field_attrs(135));
    schema.addField(
      "sort_order_id", optional_of(avro::IntSchema()), field_attrs(140));
    return schema;
}

avro::Schema manifest_entry_schema() {
    avro::RecordSchema schema("manifest_entry");
    schema.addField("status", avro::IntSchema(), field_attrs(0));
    schema.addField(
      "snapshot_id", optional_of(avro::LongSchema()), field_attrs(1));
    schema.addField(
      "sequence_number", optional_of(avro::LongSchema()), field_attrs(3));
    schema.addField(
      "file_sequence_number", optional_of(avro::LongSchema()), field_attrs(4));
    schema.addField("data_file", data_file_schema(), field_attrs(2));
    return schema;
}

} // namespace iceberg
