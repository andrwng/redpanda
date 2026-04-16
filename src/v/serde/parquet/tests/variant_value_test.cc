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
#include "serde/parquet/variant_value.h"

#include <gtest/gtest.h>

#include <memory>

using namespace serde::parquet;

TEST(VariantValue, Primitives) {
    variant_value v_null = null_value{};
    EXPECT_TRUE(std::holds_alternative<null_value>(v_null));

    variant_value v_bool = boolean_value{.val = true};
    EXPECT_TRUE(std::holds_alternative<boolean_value>(v_bool));

    variant_value v_i32 = int32_value{.val = 42};
    EXPECT_EQ(std::get<int32_value>(v_i32).val, 42);

    variant_value v_i64 = int64_value{.val = 1234567890LL};
    EXPECT_EQ(std::get<int64_value>(v_i64).val, 1234567890LL);

    variant_value v_f32 = float32_value{.val = 3.14f};
    EXPECT_FLOAT_EQ(std::get<float32_value>(v_f32).val, 3.14f);

    variant_value v_f64 = float64_value{.val = 2.718281828};
    EXPECT_DOUBLE_EQ(std::get<float64_value>(v_f64).val, 2.718281828);

    variant_value v_str = variant_string_value{.val = "hello"};
    EXPECT_EQ(std::get<variant_string_value>(v_str).val, "hello");

    iobuf buf;
    buf.append("bytes", 5);
    variant_value v_bytes = byte_array_value{.val = std::move(buf)};
    EXPECT_EQ(std::get<byte_array_value>(v_bytes).val.size_bytes(), 5);
}

TEST(VariantValue, Object) {
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("name", variant_string_value{.val = "alice"});
    obj->fields.emplace_back("age", int32_value{.val = 30});
    obj->fields.emplace_back("active", boolean_value{.val = true});

    variant_value v = std::move(obj);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<variant_object>>(v));
    const auto& o = *std::get<std::unique_ptr<variant_object>>(v);
    EXPECT_EQ(o.fields.size(), 3);
    EXPECT_EQ(o.fields[0].first, "name");
    EXPECT_EQ(o.fields[1].first, "age");
}

TEST(VariantValue, Array) {
    auto arr = std::make_unique<variant_array>();
    arr->elements.emplace_back(int32_value{.val = 1});
    arr->elements.emplace_back(variant_string_value{.val = "two"});
    arr->elements.emplace_back(boolean_value{.val = false});

    variant_value v = std::move(arr);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<variant_array>>(v));
    const auto& a = *std::get<std::unique_ptr<variant_array>>(v);
    EXPECT_EQ(a.elements.size(), 3);
}

TEST(VariantValue, NestedObjectArrayObject) {
    // { "items": [ { "id": 1 }, { "id": 2 } ] }
    auto inner1 = std::make_unique<variant_object>();
    inner1->fields.emplace_back("id", int32_value{.val = 1});

    auto inner2 = std::make_unique<variant_object>();
    inner2->fields.emplace_back("id", int32_value{.val = 2});

    auto arr = std::make_unique<variant_array>();
    arr->elements.emplace_back(std::move(inner1));
    arr->elements.emplace_back(std::move(inner2));

    auto outer = std::make_unique<variant_object>();
    outer->fields.emplace_back("items", std::move(arr));

    variant_value v = std::move(outer);
    ASSERT_TRUE(std::holds_alternative<std::unique_ptr<variant_object>>(v));

    const auto& root = *std::get<std::unique_ptr<variant_object>>(v);
    ASSERT_EQ(root.fields.size(), 1);
    EXPECT_EQ(root.fields[0].first, "items");

    const auto& items_arr = *std::get<std::unique_ptr<variant_array>>(
      root.fields[0].second);
    ASSERT_EQ(items_arr.elements.size(), 2);

    const auto& item0 = *std::get<std::unique_ptr<variant_object>>(
      items_arr.elements[0]);
    EXPECT_EQ(std::get<int32_value>(item0.fields[0].second).val, 1);
}

TEST(VariantValue, EqualitySameStructure) {
    auto make_obj = []() {
        auto obj = std::make_unique<variant_object>();
        obj->fields.emplace_back("x", int32_value{.val = 10});
        obj->fields.emplace_back("y", variant_string_value{.val = "hello"});
        return variant_value{std::move(obj)};
    };
    auto a = make_obj();
    auto b = make_obj();
    EXPECT_TRUE(a == b);
}

TEST(VariantValue, EqualityDifferentValues) {
    variant_value a = int32_value{.val = 1};
    variant_value b = int32_value{.val = 2};
    EXPECT_FALSE(a == b);
}

TEST(VariantValue, EqualityDifferentTypes) {
    variant_value a = int32_value{.val = 1};
    variant_value b = int64_value{.val = 1};
    EXPECT_FALSE(a == b);
}

TEST(VariantValue, EqualityNestedArrays) {
    auto make_arr = []() {
        auto arr = std::make_unique<variant_array>();
        arr->elements.emplace_back(int32_value{.val = 1});
        arr->elements.emplace_back(int32_value{.val = 2});
        return variant_value{std::move(arr)};
    };
    EXPECT_TRUE(make_arr() == make_arr());

    auto arr_different = std::make_unique<variant_array>();
    arr_different->elements.emplace_back(int32_value{.val = 1});
    arr_different->elements.emplace_back(int32_value{.val = 99});
    variant_value c = std::move(arr_different);

    EXPECT_FALSE(make_arr() == c);
}

TEST(VariantValue, CopyPrimitive) {
    variant_value orig = int32_value{.val = 42};
    auto copied = copy_variant(orig);
    EXPECT_TRUE(orig == copied);
}

TEST(VariantValue, CopyByteArray) {
    iobuf buf;
    buf.append("data", 4);
    variant_value orig = byte_array_value{.val = std::move(buf)};
    auto copied = copy_variant(orig);
    EXPECT_TRUE(orig == copied);
}

TEST(VariantValue, CopyObjectIndependent) {
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("key", int32_value{.val = 100});
    variant_value orig = std::move(obj);

    auto copied = copy_variant(orig);
    EXPECT_TRUE(orig == copied);

    // Mutate the copy and verify independence.
    auto& copied_obj = *std::get<std::unique_ptr<variant_object>>(copied);
    copied_obj.fields[0].second = int32_value{.val = 999};
    EXPECT_FALSE(orig == copied);
}

TEST(VariantValue, CopyNestedArray) {
    auto inner = std::make_unique<variant_object>();
    inner->fields.emplace_back("v", boolean_value{.val = true});

    auto arr = std::make_unique<variant_array>();
    arr->elements.emplace_back(std::move(inner));
    arr->elements.emplace_back(int64_value{.val = 7});

    variant_value orig = std::move(arr);
    auto copied = copy_variant(orig);
    EXPECT_TRUE(orig == copied);
}

TEST(VariantValue, SortObject) {
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("zebra", int32_value{.val = 3});
    obj->fields.emplace_back("apple", int32_value{.val = 1});
    obj->fields.emplace_back("mango", int32_value{.val = 2});

    sort_variant_object(*obj);

    ASSERT_EQ(obj->fields.size(), 3);
    EXPECT_EQ(obj->fields[0].first, "apple");
    EXPECT_EQ(obj->fields[1].first, "mango");
    EXPECT_EQ(obj->fields[2].first, "zebra");
    EXPECT_EQ(std::get<int32_value>(obj->fields[0].second).val, 1);
    EXPECT_EQ(std::get<int32_value>(obj->fields[1].second).val, 2);
    EXPECT_EQ(std::get<int32_value>(obj->fields[2].second).val, 3);
}

TEST(VariantValue, SortEmptyObject) {
    variant_object obj;
    sort_variant_object(obj);
    EXPECT_TRUE(obj.fields.empty());
}
