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
#include "bytes/iobuf_parser.h"
#include "serde/parquet/variant_encoding.h"
#include "serde/parquet/variant_value.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

using namespace serde::parquet;

namespace {

void round_trip(const variant_value& input) {
    auto encoded = encode_variant(input);
    auto decoded = decode_variant(encoded.metadata, encoded.value);
    EXPECT_TRUE(input == decoded);
}

} // namespace

TEST(VariantEncoding, Null) { round_trip(null_value{}); }

TEST(VariantEncoding, True) { round_trip(boolean_value{.val = true}); }

TEST(VariantEncoding, False) { round_trip(boolean_value{.val = false}); }

TEST(VariantEncoding, Int32) { round_trip(int32_value{.val = -42}); }

TEST(VariantEncoding, Int32Zero) { round_trip(int32_value{.val = 0}); }

TEST(VariantEncoding, Int32Max) {
    round_trip(int32_value{.val = std::numeric_limits<int32_t>::max()});
}

TEST(VariantEncoding, Int64) {
    round_trip(int64_value{.val = 1234567890123LL});
}

TEST(VariantEncoding, Int64Negative) {
    round_trip(int64_value{.val = -9876543210LL});
}

TEST(VariantEncoding, Float32) { round_trip(float32_value{.val = 3.14f}); }

TEST(VariantEncoding, Float32NegZero) {
    round_trip(float32_value{.val = -0.0f});
}

TEST(VariantEncoding, Float64) {
    round_trip(float64_value{.val = 2.718281828459045});
}

TEST(VariantEncoding, Float64Infinity) {
    round_trip(float64_value{.val = std::numeric_limits<double>::infinity()});
}

TEST(VariantEncoding, ShortStringEmpty) {
    round_trip(variant_string_value{.val = ""});
}

TEST(VariantEncoding, ShortString) {
    round_trip(variant_string_value{.val = "hello world"});
}

TEST(VariantEncoding, ShortStringMax63) {
    // 63-byte string encodes as short_string
    std::string s(63, 'x');
    round_trip(variant_string_value{.val = ss::sstring(s)});
}

TEST(VariantEncoding, LongString64) {
    // 64-byte string encodes as primitive string
    std::string s(64, 'y');
    round_trip(variant_string_value{.val = ss::sstring(s)});
}

TEST(VariantEncoding, LongStringLarge) {
    std::string s(1000, 'z');
    round_trip(variant_string_value{.val = ss::sstring(s)});
}

TEST(VariantEncoding, BinaryEmpty) {
    iobuf buf;
    round_trip(byte_array_value{.val = std::move(buf)});
}

TEST(VariantEncoding, BinaryData) {
    iobuf buf;
    buf.append("\x00\x01\x02\xff", 4);
    round_trip(byte_array_value{.val = std::move(buf)});
}

TEST(VariantEncoding, EmptyObject) {
    auto obj = std::make_unique<variant_object>();
    round_trip(std::move(obj));
}

TEST(VariantEncoding, ObjectWithFields) {
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("name", variant_string_value{.val = "alice"});
    obj->fields.emplace_back("age", int32_value{.val = 30});
    obj->fields.emplace_back("active", boolean_value{.val = true});
    sort_variant_object(*obj);
    round_trip(std::move(obj));
}

TEST(VariantEncoding, EmptyArray) {
    auto arr = std::make_unique<variant_array>();
    round_trip(std::move(arr));
}

TEST(VariantEncoding, ArrayMixedTypes) {
    auto arr = std::make_unique<variant_array>();
    arr->elements.emplace_back(int32_value{.val = 1});
    arr->elements.emplace_back(variant_string_value{.val = "two"});
    arr->elements.emplace_back(boolean_value{.val = false});
    arr->elements.emplace_back(null_value{});
    arr->elements.emplace_back(float64_value{.val = 4.0});
    round_trip(std::move(arr));
}

TEST(VariantEncoding, NestedObjectArrayObject) {
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

    round_trip(std::move(outer));
}

TEST(VariantEncoding, DictionarySortedDeduplicated) {
    // Object with keys "b", "a", "b" should produce dictionary ["a", "b"]
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("b", int32_value{.val = 1});
    obj->fields.emplace_back("a", int32_value{.val = 2});
    obj->fields.emplace_back("b", int32_value{.val = 3});

    auto encoded = encode_variant(variant_value{std::move(obj)});

    // Parse the metadata to verify dictionary contents.
    iobuf_const_parser p(encoded.metadata);
    uint8_t header = p.consume_type<uint8_t>();
    uint8_t offset_size = static_cast<uint8_t>(((header >> 5) & 0x03) + 1);
    uint8_t raw[4] = {};
    p.consume_to(offset_size, raw);
    uint32_t dict_size = 0;
    std::memcpy(&dict_size, raw, 4);
    EXPECT_EQ(dict_size, 2);

    // Read offsets
    chunked_vector<uint32_t> offsets;
    for (uint32_t i = 0; i <= dict_size; ++i) {
        uint8_t obuf[4] = {};
        p.consume_to(offset_size, obuf);
        uint32_t val = 0;
        std::memcpy(&val, obuf, 4);
        offsets.push_back(val);
    }

    // Read strings
    chunked_vector<ss::sstring> strings;
    for (uint32_t i = 0; i < dict_size; ++i) {
        uint32_t len = offsets[i + 1] - offsets[i];
        strings.push_back(p.read_string_unsafe(len));
    }

    ASSERT_EQ(strings.size(), 2);
    EXPECT_EQ(strings[0], "a");
    EXPECT_EQ(strings[1], "b");
}

TEST(VariantEncoding, DeeplyNested) {
    // Array of arrays of objects
    auto obj1 = std::make_unique<variant_object>();
    obj1->fields.emplace_back("x", int64_value{.val = 100});

    auto inner_arr = std::make_unique<variant_array>();
    inner_arr->elements.emplace_back(std::move(obj1));
    inner_arr->elements.emplace_back(variant_string_value{.val = "inner"});

    auto outer_arr = std::make_unique<variant_array>();
    outer_arr->elements.emplace_back(std::move(inner_arr));
    outer_arr->elements.emplace_back(int32_value{.val = 42});

    round_trip(std::move(outer_arr));
}

TEST(VariantEncoding, ObjectFieldOrderPreserved) {
    auto obj = std::make_unique<variant_object>();
    obj->fields.emplace_back("z", int32_value{.val = 1});
    obj->fields.emplace_back("a", int32_value{.val = 2});
    obj->fields.emplace_back("m", int32_value{.val = 3});

    auto encoded = encode_variant(variant_value{std::move(obj)});
    auto decoded = decode_variant(encoded.metadata, encoded.value);

    const auto& dec_obj = *std::get<std::unique_ptr<variant_object>>(decoded);
    ASSERT_EQ(dec_obj.fields.size(), 3);
    EXPECT_EQ(dec_obj.fields[0].first, "z");
    EXPECT_EQ(dec_obj.fields[1].first, "a");
    EXPECT_EQ(dec_obj.fields[2].first, "m");
}
