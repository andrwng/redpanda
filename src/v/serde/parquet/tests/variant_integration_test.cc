/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iostream.h"
#include "serde/parquet/reader.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/variant_encoding.h"
#include "serde/parquet/variant_json.h"
#include "serde/parquet/variant_schema.h"
#include "serde/parquet/variant_value.h"
#include "serde/parquet/writer.h"

#include <gtest/gtest.h>

namespace serde::parquet {

namespace {

schema_element make_root(schema_element child) {
    schema_element root{
      .repetition_type = field_repetition_type::required,
      .path = {"root"},
    };
    root.children.push_back(std::move(child));
    return root;
}

iobuf write_single_row(schema_element schema, group_value row) {
    group_value top;
    top.push_back(group_member{value{std::move(row)}});
    iobuf file;
    writer w({.schema = std::move(schema)}, make_iobuf_ref_output_stream(file));
    w.init().get();
    w.write_row(std::move(top)).get();
    w.close().get();
    return file;
}

} // namespace

// NOLINTBEGIN(*magic-number*)

TEST(VariantIntegration, UnshredddedWriteRead) {
    auto json = iobuf::from(R"({"name":"alice","age":30})");
    auto val = parse_json_to_variant(std::move(json)).get();

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(val));
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);

    // Top-level record has one member: the variant group.
    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);
    ASSERT_EQ(variant_group->size(), 2);

    // Extract metadata and value byte arrays.
    const auto* meta_ba = std::get_if<byte_array_value>(
      &(*variant_group)[0].field);
    const auto* val_ba = std::get_if<byte_array_value>(
      &(*variant_group)[1].field);
    ASSERT_NE(meta_ba, nullptr);
    ASSERT_NE(val_ba, nullptr);

    // Decode and compare to original.
    auto decoded = decode_variant(meta_ba->val, val_ba->val);
    EXPECT_EQ(decoded, val);
}

TEST(VariantIntegration, ShreddedWriteRead) {
    auto json = iobuf::from(R"({"name":"alice","age":30,"extra":true})");
    auto val = parse_json_to_variant(std::move(json)).get();

    variant_shredding_schema shredding;
    variant_shredding_field age_field;
    age_field.field_path.push_back("age");
    age_field.type = i32_type{};
    shredding.fields.push_back(std::move(age_field));

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required, shredding);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(val), shredding);
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);

    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);
    // 3 children: metadata, value, typed_value
    ASSERT_EQ(variant_group->size(), 3);

    // typed_value group contains the shredded age field.
    const auto* typed_value_group = std::get_if<group_value>(
      &(*variant_group)[2].field);
    ASSERT_NE(typed_value_group, nullptr);
    ASSERT_EQ(typed_value_group->size(), 1);

    const auto* age_val = std::get_if<int32_value>(
      &(*typed_value_group)[0].field);
    ASSERT_NE(age_val, nullptr);
    EXPECT_EQ(age_val->val, 30);

    // The residual should still be decodable and contain name + extra.
    const auto* meta_ba = std::get_if<byte_array_value>(
      &(*variant_group)[0].field);
    ASSERT_NE(meta_ba, nullptr);

    // Value could be byte_array (non-trivial residual) or null (trivial).
    // With name + extra remaining, it should be non-trivial.
    const auto* val_ba = std::get_if<byte_array_value>(
      &(*variant_group)[1].field);
    ASSERT_NE(val_ba, nullptr);

    auto residual = decode_variant(meta_ba->val, val_ba->val);
    // The residual should be an object without "age".
    const auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(
      &residual);
    ASSERT_NE(obj_ptr, nullptr);
    ASSERT_NE(*obj_ptr, nullptr);
    for (const auto& [key, _] : (*obj_ptr)->fields) {
        EXPECT_NE(key, "age");
    }
}

TEST(VariantIntegration, ShreddedNestedPath) {
    auto json = iobuf::from(R"({"event":{"type":"click","ts":42},"id":1})");
    auto val = parse_json_to_variant(std::move(json)).get();

    variant_shredding_schema shredding;
    variant_shredding_field type_field;
    type_field.field_path.push_back("event");
    type_field.field_path.push_back("type");
    type_field.type = byte_array_type{};
    shredding.fields.push_back(std::move(type_field));

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required, shredding);

    // Verify schema structure: typed_value -> event (group) -> type (leaf).
    ASSERT_EQ(variant_elem.children.size(), 3);
    const auto& tv = variant_elem.children[2];
    EXPECT_EQ(tv.name(), "typed_value");
    ASSERT_EQ(tv.children.size(), 1);
    EXPECT_EQ(tv.children[0].name(), "event");
    ASSERT_EQ(tv.children[0].children.size(), 1);
    EXPECT_EQ(tv.children[0].children[0].name(), "type");
    EXPECT_TRUE(
      std::holds_alternative<string_type>(
        tv.children[0].children[0].logical_type));

    auto schema = make_root(std::move(variant_elem));
    auto row = encode_variant_for_writer(copy_variant(val), shredding);
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);

    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* vg = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(vg, nullptr);
    ASSERT_EQ(vg->size(), 3);

    // typed_value -> event group -> type leaf
    const auto* tv_group = std::get_if<group_value>(&(*vg)[2].field);
    ASSERT_NE(tv_group, nullptr);
    ASSERT_EQ(tv_group->size(), 1);
    const auto* event_group = std::get_if<group_value>(&(*tv_group)[0].field);
    ASSERT_NE(event_group, nullptr);
    ASSERT_EQ(event_group->size(), 1);
    const auto* type_val = std::get_if<byte_array_value>(
      &(*event_group)[0].field);
    ASSERT_NE(type_val, nullptr);

    iobuf expected_val = iobuf::from("click");
    EXPECT_EQ(type_val->val, expected_val);
}

TEST(VariantIntegration, UnshredddedDecodeFromReader) {
    auto json = iobuf::from(R"({"name":"alice","age":30})");
    auto original = parse_json_to_variant(std::move(json)).get();

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(original));
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);
    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);

    auto decoded = decode_variant_from_reader(*variant_group);
    EXPECT_EQ(decoded, original);
}

TEST(VariantIntegration, ShreddedRoundTrip) {
    auto json = iobuf::from(R"({"name":"alice","age":30,"score":95.5})");
    auto original = parse_json_to_variant(std::move(json)).get();

    variant_shredding_schema shredding;
    variant_shredding_field age_field;
    age_field.field_path.push_back("age");
    age_field.type = i32_type{};
    shredding.fields.push_back(std::move(age_field));

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required, shredding);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(original), shredding);
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);
    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);

    auto decoded = decode_variant_from_reader(*variant_group, shredding);
    EXPECT_EQ(decoded, original);
}

TEST(VariantIntegration, ShreddedNestedPathRoundTrip) {
    auto json = iobuf::from(R"({"event":{"type":"click","ts":42},"id":1})");
    auto original = parse_json_to_variant(std::move(json)).get();

    variant_shredding_schema shredding;
    variant_shredding_field type_field;
    type_field.field_path.push_back("event");
    type_field.field_path.push_back("type");
    type_field.type = byte_array_type{};
    shredding.fields.push_back(std::move(type_field));

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required, shredding);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(original), shredding);
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);
    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);

    auto decoded = decode_variant_from_reader(*variant_group, shredding);
    EXPECT_EQ(decoded, original);
}

TEST(VariantIntegration, ShreddedTrivialResidualRoundTrip) {
    auto json = iobuf::from(R"({"age":30})");
    auto original = parse_json_to_variant(std::move(json)).get();

    variant_shredding_schema shredding;
    variant_shredding_field age_field;
    age_field.field_path.push_back("age");
    age_field.type = i32_type{};
    shredding.fields.push_back(std::move(age_field));

    auto variant_elem = build_variant_schema(
      "data", field_repetition_type::required, shredding);
    auto schema = make_root(std::move(variant_elem));

    auto row = encode_variant_for_writer(copy_variant(original), shredding);
    auto file = write_single_row(std::move(schema), std::move(row));

    auto records = read_file_as_records(std::move(file)).get();
    ASSERT_EQ(records.size(), 1);
    const auto& top = records[0];
    ASSERT_EQ(top.size(), 1);
    const auto* variant_group = std::get_if<group_value>(&top[0].field);
    ASSERT_NE(variant_group, nullptr);

    auto decoded = decode_variant_from_reader(*variant_group, shredding);
    EXPECT_EQ(decoded, original);
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
