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

#include "serde/parquet/variant_encoding.h"
#include "serde/parquet/variant_json.h"
#include "serde/parquet/variant_value.h"

#include <gtest/gtest.h>

using namespace serde::parquet;

TEST(VariantJson, Null) {
    auto v = parse_json_to_variant(iobuf::from("null")).get();
    EXPECT_TRUE(std::holds_alternative<null_value>(v));
}

TEST(VariantJson, BoolTrue) {
    auto v = parse_json_to_variant(iobuf::from("true")).get();
    EXPECT_EQ(std::get<boolean_value>(v), (boolean_value{true}));
}

TEST(VariantJson, BoolFalse) {
    auto v = parse_json_to_variant(iobuf::from("false")).get();
    EXPECT_EQ(std::get<boolean_value>(v), (boolean_value{false}));
}

TEST(VariantJson, Integer) {
    auto v = parse_json_to_variant(iobuf::from("42")).get();
    EXPECT_EQ(std::get<int32_value>(v), (int32_value{42}));
}

TEST(VariantJson, NegativeInteger) {
    auto v = parse_json_to_variant(iobuf::from("-100")).get();
    EXPECT_EQ(std::get<int32_value>(v), (int32_value{-100}));
}

TEST(VariantJson, LargeInteger) {
    auto v = parse_json_to_variant(iobuf::from("3000000000")).get();
    EXPECT_EQ(std::get<int64_value>(v), (int64_value{3000000000}));
}

TEST(VariantJson, Double) {
    auto v = parse_json_to_variant(iobuf::from("3.14")).get();
    EXPECT_EQ(std::get<float64_value>(v).val, 3.14);
}

TEST(VariantJson, String) {
    auto v = parse_json_to_variant(iobuf::from("\"hello\"")).get();
    EXPECT_EQ(std::get<variant_string_value>(v).val, "hello");
}

TEST(VariantJson, EmptyString) {
    auto v = parse_json_to_variant(iobuf::from("\"\"")).get();
    EXPECT_EQ(std::get<variant_string_value>(v).val, "");
}

TEST(VariantJson, Object) {
    auto v = parse_json_to_variant(iobuf::from(R"({"b":2,"a":1})")).get();
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&v);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 2);
    EXPECT_EQ((*obj)->fields[0].first, "a");
    EXPECT_EQ((*obj)->fields[1].first, "b");
}

TEST(VariantJson, EmptyObject) {
    auto v = parse_json_to_variant(iobuf::from("{}")).get();
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&v);
    ASSERT_NE(obj, nullptr);
    EXPECT_EQ((*obj)->fields.size(), 0);
}

TEST(VariantJson, Array) {
    auto v = parse_json_to_variant(iobuf::from(R"([1,"two",null])")).get();
    auto* arr = std::get_if<std::unique_ptr<variant_array>>(&v);
    ASSERT_NE(arr, nullptr);
    ASSERT_EQ((*arr)->elements.size(), 3);
    EXPECT_TRUE(std::holds_alternative<int32_value>((*arr)->elements[0]));
    EXPECT_TRUE(
      std::holds_alternative<variant_string_value>((*arr)->elements[1]));
    EXPECT_TRUE(std::holds_alternative<null_value>((*arr)->elements[2]));
}

TEST(VariantJson, EmptyArray) {
    auto v = parse_json_to_variant(iobuf::from("[]")).get();
    auto* arr = std::get_if<std::unique_ptr<variant_array>>(&v);
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ((*arr)->elements.size(), 0);
}

TEST(VariantJson, Nested) {
    auto v = parse_json_to_variant(iobuf::from(R"({"a":{"b":[1,2,3]}})")).get();
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&v);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "a");
    auto* inner = std::get_if<std::unique_ptr<variant_object>>(
      &(*obj)->fields[0].second);
    ASSERT_NE(inner, nullptr);
    ASSERT_EQ((*inner)->fields.size(), 1);
    auto* arr = std::get_if<std::unique_ptr<variant_array>>(
      &(*inner)->fields[0].second);
    ASSERT_NE(arr, nullptr);
    EXPECT_EQ((*arr)->elements.size(), 3);
}

TEST(VariantJson, RoundTripThroughEncoding) {
    auto v = parse_json_to_variant(
               iobuf::from(R"({"name":"alice","age":30,"scores":[95,87]})"))
               .get();
    auto encoded = encode_variant(v);
    auto decoded = decode_variant(encoded.metadata, encoded.value);
    EXPECT_EQ(v, decoded);
}
