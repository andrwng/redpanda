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

#include "bytes/iobuf.h"
#include "serde/parquet/variant_shredder.h"
#include "serde/parquet/variant_value.h"

#include <gtest/gtest.h>

using namespace serde::parquet;

namespace {

variant_value
make_object(chunked_vector<std::pair<ss::sstring, variant_value>> fields) {
    auto obj = std::make_unique<variant_object>();
    obj->fields = std::move(fields);
    return obj;
}

chunked_vector<ss::sstring> path(std::initializer_list<ss::sstring> parts) {
    chunked_vector<ss::sstring> result;
    for (auto& p : parts) {
        result.push_back(p);
    }
    return result;
}

} // namespace

TEST(VariantShredder, TopLevelField) {
    // {"a":1, "b":2} with schema [{path:["a"], type:i32}]
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("a", int32_value{1});
    fields.emplace_back("b", int32_value{2});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"a"});
    f.type = i32_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(result.typed_values[0], value{int32_value{1}});

    // Residual should be {"b":2}
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "b");
    EXPECT_EQ((*obj)->fields[0].second, variant_value{int32_value{2}});
}

TEST(VariantShredder, NestedField) {
    // {"x":{"y":1}} with schema [{path:["x","y"], type:i32}]
    chunked_vector<std::pair<ss::sstring, variant_value>> inner_fields;
    inner_fields.emplace_back("y", int32_value{1});
    auto inner = make_object(std::move(inner_fields));

    chunked_vector<std::pair<ss::sstring, variant_value>> outer_fields;
    outer_fields.emplace_back("x", std::move(inner));
    auto val = make_object(std::move(outer_fields));

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"x", "y"});
    f.type = i32_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(result.typed_values[0], value{int32_value{1}});

    // Residual should be {"x":{}}
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "x");
    auto* inner_obj = std::get_if<std::unique_ptr<variant_object>>(
      &(*obj)->fields[0].second);
    ASSERT_NE(inner_obj, nullptr);
    EXPECT_TRUE((*inner_obj)->fields.empty());
}

TEST(VariantShredder, AbsentField) {
    // {"a":1} with schema [{path:["b"], type:i32}]
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("a", int32_value{1});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"b"});
    f.type = i32_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(result.typed_values[0], value{null_value{}});

    // Residual unchanged: {"a":1}
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "a");
}

TEST(VariantShredder, TypeMismatch) {
    // {"a":"string"} with schema [{path:["a"], type:i32}]
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("a", variant_string_value{"string"});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"a"});
    f.type = i32_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(result.typed_values[0], value{null_value{}});

    // Residual still has {"a":"string"}
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "a");
    EXPECT_EQ(
      (*obj)->fields[0].second, variant_value{variant_string_value{"string"}});
}

TEST(VariantShredder, EmptySchema) {
    // {"a":1} with empty schema
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("a", int32_value{1});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;

    auto result = shred_variant(std::move(val), schema);

    EXPECT_TRUE(result.typed_values.empty());

    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "a");
}

TEST(VariantShredder, MultipleFields) {
    // {"a":1, "b":"hello", "c":3.14} with schema
    //   [{path:["a"],i32}, {path:["b"],byte_array}]
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("a", int32_value{1});
    fields.emplace_back("b", variant_string_value{"hello"});
    fields.emplace_back("c", float64_value{3.14});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;
    {
        variant_shredding_field f;
        f.field_path = path({"a"});
        f.type = i32_type{};
        schema.fields.push_back(std::move(f));
    }
    {
        variant_shredding_field f;
        f.field_path = path({"b"});
        f.type = byte_array_type{};
        schema.fields.push_back(std::move(f));
    }

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 2);
    EXPECT_EQ(result.typed_values[0], value{int32_value{1}});
    EXPECT_EQ(
      result.typed_values[1], value{byte_array_value{iobuf::from("hello")}});

    // Residual should be {"c":3.14}
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    ASSERT_EQ((*obj)->fields.size(), 1);
    EXPECT_EQ((*obj)->fields[0].first, "c");
}

TEST(VariantShredder, StringAsBytes) {
    // {"s":"text"} with schema [{path:["s"], type:byte_array}]
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    fields.emplace_back("s", variant_string_value{"text"});
    auto val = make_object(std::move(fields));

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"s"});
    f.type = byte_array_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(
      result.typed_values[0], value{byte_array_value{iobuf::from("text")}});

    // Residual should be empty object
    auto* obj = std::get_if<std::unique_ptr<variant_object>>(&result.residual);
    ASSERT_NE(obj, nullptr);
    EXPECT_TRUE((*obj)->fields.empty());
}

TEST(VariantShredder, NonObjectInput) {
    // int32_value{42} with schema [{path:["a"], type:i32}]
    variant_value val = int32_value{42};

    variant_shredding_schema schema;
    variant_shredding_field f;
    f.field_path = path({"a"});
    f.type = i32_type{};
    schema.fields.push_back(std::move(f));

    auto result = shred_variant(std::move(val), schema);

    ASSERT_EQ(result.typed_values.size(), 1);
    EXPECT_EQ(result.typed_values[0], value{null_value{}});

    // Residual is the original primitive
    EXPECT_EQ(result.residual, variant_value{int32_value{42}});
}
