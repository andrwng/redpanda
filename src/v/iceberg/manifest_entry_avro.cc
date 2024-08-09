// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/manifest_entry_avro.h"

#include "iceberg/avro_utils.h"
#include "iceberg/datatypes.h"
#include "iceberg/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/schema_avro.h"
#include "iceberg/values.h"

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

avro::Schema partition_schema(const struct_type& partition_type) {
    return schema_as_avro(partition_type, "r102");
}

avro::Schema data_file_schema(const struct_type& partition_type) {
    avro::RecordSchema schema("r2");
    schema.addField("conent", avro::IntSchema(), field_attrs(134));
    schema.addField("file_path", avro::StringSchema(), field_attrs(100));
    schema.addField("file_format", avro::StringSchema(), field_attrs(101));
    schema.addField(
      "partition", partition_schema(partition_type), field_attrs(102));
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
        "k119_v120", 119, avro::IntSchema(), 120, avro::LongSchema()))),
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

avro::Schema manifest_entry_schema(const struct_type& partition_type) {
    avro::RecordSchema schema("manifest_entry");
    schema.addField("status", avro::IntSchema(), field_attrs(0));
    schema.addField(
      "snapshot_id", optional_of(avro::LongSchema()), field_attrs(1));
    schema.addField(
      "sequence_number", optional_of(avro::LongSchema()), field_attrs(3));
    schema.addField(
      "file_sequence_number", optional_of(avro::LongSchema()), field_attrs(4));
    schema.addField(
      "data_file", data_file_schema(partition_type), field_attrs(2));
    return schema;
}

struct_type data_file_type(struct_type partition_type) {
    struct_type r2_type;
    r2_type.fields.emplace_back(
      nested_field::create(134, "content", field_required::yes, int_type()));
    r2_type.fields.emplace_back(nested_field::create(
      100, "file_path", field_required::yes, string_type()));
    r2_type.fields.emplace_back(nested_field::create(
      101, "file_format", field_required::yes, string_type()));
    r2_type.fields.emplace_back(nested_field::create(
      102, "partition", field_required::yes, std::move(partition_type)));
    r2_type.fields.emplace_back(nested_field::create(
      103, "record_count", field_required::yes, long_type()));
    r2_type.fields.emplace_back(nested_field::create(
      104, "file_size_in_bytes", field_required::yes, long_type()));
    r2_type.fields.emplace_back(nested_field::create(
      108,
      "column_sizes",
      field_required::no,
      map_type::create(
        117, int_type(), 118, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      109,
      "value_counts",
      field_required::no,
      map_type::create(
        119, int_type(), 120, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      110,
      "null_value_counts",
      field_required::no,
      map_type::create(
        121, int_type(), 122, field_required::yes, long_type())));
    // NOTE: distinct_counts(111) left out, since it doesn't seem widely
    // adopted across implementations.
    r2_type.fields.emplace_back(nested_field::create(
      137,
      "nan_value_counts",
      field_required::no,
      map_type::create(
        138, int_type(), 139, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      125,
      "lower_bounds",
      field_required::no,
      map_type::create(
        126, int_type(), 127, field_required::yes, binary_type())));
    r2_type.fields.emplace_back(nested_field::create(
      128,
      "upper_bounds",
      field_required::no,
      map_type::create(
        129, int_type(), 130, field_required::yes, binary_type())));
    r2_type.fields.emplace_back(nested_field::create(
      131, "key_metadata", field_required::no, binary_type()));
    r2_type.fields.emplace_back(nested_field::create(
      132,
      "split_offsets",
      field_required::no,
      list_type::create(133, field_required::yes, long_type())));
    r2_type.fields.emplace_back(nested_field::create(
      135,
      "split_offsets",
      field_required::no,
      list_type::create(136, field_required::yes, int_type())));
    r2_type.fields.emplace_back(nested_field::create(
      140, "sort_order_id", field_required::no, int_type()));
    return r2_type;
}

struct_type manifest_entry_type(struct_type partition_type) {
    struct_type manifest_entry_type;
    manifest_entry_type.fields.emplace_back(
      nested_field::create(0, "status", field_required::yes, int_type()));
    manifest_entry_type.fields.emplace_back(
      nested_field::create(1, "snapshot_id", field_required::no, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      3, "sequence_number", field_required::yes, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      4, "file_sequence_number", field_required::no, long_type()));
    manifest_entry_type.fields.emplace_back(nested_field::create(
      2,
      "data_file",
      field_required::yes,
      data_file_type(std::move(partition_type))));
    return manifest_entry_type;
}

int content_to_int(data_file_content_type c) {
    switch (c) {
    case data_file_content_type::data:
        return 0;
    case data_file_content_type::position_deletes:
        return 1;
    case data_file_content_type::equality_deletes:
        return 2;
    }
}

iobuf format_to_str(data_file_format f) {
    switch (f) {
    case data_file_format::avro:
        return iobuf::from("avro");
    case data_file_format::orc:
        return iobuf::from("orc");
    case data_file_format::parquet:
        return iobuf::from("parquet");
    }
}

struct_value data_file_to_value(data_file file) {
    struct_value ret;
    ret.fields.emplace_back(
      std::make_unique<value>(int_value(content_to_int(file.content_type))));
    ret.fields.emplace_back(
      std::make_unique<value>(string_value(iobuf::from(file.file_path))));
    ret.fields.emplace_back(
      std::make_unique<value>(string_value(format_to_str(file.file_format))));
    ret.fields.emplace_back(
      std::make_unique<value>(std::move(file.partition.val)));
    ret.fields.emplace_back(std::make_unique<value>(
      long_value(static_cast<int64_t>(file.record_count))));
    ret.fields.emplace_back(std::make_unique<value>(
      long_value(static_cast<int64_t>(file.file_size_bytes))));
    // TODO: translate all the maps.
    return ret;
}

int status_to_int(manifest_entry_status s) {
    switch (s) {
    case manifest_entry_status::existing:
        return 0;
    case manifest_entry_status::added:
        return 1;
    case manifest_entry_status::deleted:
        return 2;
    }
}

struct_value manifest_entry_to_value(manifest_entry entry) {
    struct_value ret;
    ret.fields.emplace_back(
      std::make_unique<value>(int_value(status_to_int(entry.status))));
    if (entry.snapshot_id.has_value()) {
        ret.fields.emplace_back(
          std::make_unique<value>(long_value(*entry.snapshot_id)));
    }
    if (entry.sequence_number.has_value()) {
        ret.fields.emplace_back(
          std::make_unique<value>(long_value(*entry.sequence_number)));
    }
    if (entry.file_sequence_number.has_value()) {
        ret.fields.emplace_back(
          std::make_unique<value>(long_value(*entry.file_sequence_number)));
    }
    ret.fields.emplace_back(
      std::make_unique<value>(data_file_to_value(std::move(entry.data_file))));
    return ret;
}

} // namespace iceberg
