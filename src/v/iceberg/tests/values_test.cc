// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/values.h"

#include <gtest/gtest.h>

using namespace iceberg;

// Returns a list of unique primitive values.
chunked_vector<value> unique_values_primitive_types() {
    chunked_vector<value> vals;
    vals.emplace_back(boolean_value{true});
    vals.emplace_back(boolean_value{false});

    vals.emplace_back(int_value{0});
    vals.emplace_back(int_value{1});

    vals.emplace_back(long_value{0});
    vals.emplace_back(long_value{1});

    vals.emplace_back(float_value{0.0});
    vals.emplace_back(float_value{1.0});

    vals.emplace_back(double_value{0.0});
    vals.emplace_back(double_value{1.0});

    vals.emplace_back(decimal_value{0});
    vals.emplace_back(decimal_value{1});

    vals.emplace_back(date_value{0});
    vals.emplace_back(date_value{1});

    vals.emplace_back(time_value{0});
    vals.emplace_back(time_value{1});

    vals.emplace_back(timestamp_value{0});
    vals.emplace_back(timestamp_value{1});

    vals.emplace_back(timestamptz_value{0});
    vals.emplace_back(timestamptz_value{1});

    vals.emplace_back(string_value{iobuf{}});
    vals.emplace_back(string_value{iobuf::from("0")});
    vals.emplace_back(string_value{iobuf::from("1")});

    vals.emplace_back(binary_value{iobuf{}});
    vals.emplace_back(binary_value{iobuf::from("0")});
    vals.emplace_back(binary_value{iobuf::from("1")});

    vals.emplace_back(uuid_value{uuid_t{}});
    vals.emplace_back(uuid_value{uuid_t::create()});

    vals.emplace_back(fixed_value{iobuf{}});
    vals.emplace_back(fixed_value{iobuf::from("0")});
    vals.emplace_back(fixed_value{iobuf::from("1")});
    return vals;
};

void check_single_value_exists(
  const value& expected_val, const chunked_vector<value>& all_unique_vals) {
    size_t num_eq = 0;
    size_t num_ne = 0;
    for (const auto& v : all_unique_vals) {
        if (v == expected_val) {
            ++num_eq;
        }
        if (v != expected_val) {
            ++num_ne;
        }
    }
    ASSERT_EQ(num_eq, 1);
    ASSERT_EQ(num_ne, all_unique_vals.size() - 1);
}

TEST(ValuesTest, TestPrimitiveValuesEquality) {
    auto vals = unique_values_primitive_types();
    for (const auto& v : vals) {
        ASSERT_NO_FATAL_FAILURE(check_single_value_exists(v, vals));
    }
}

TEST(ValuesTest, TestStructEquality) {
    struct_value v1;
    v1.fields.emplace_back(std::make_unique<value>(int_value{0}));
    v1.fields.emplace_back(std::make_unique<value>(boolean_value{false}));
    ASSERT_EQ(v1, v1);

    struct_value v1_copy;
    v1_copy.fields.emplace_back(std::make_unique<value>(int_value{0}));
    v1_copy.fields.emplace_back(std::make_unique<value>(boolean_value{false}));
    ASSERT_EQ(v1, v1_copy);

    struct_value v2;
    v2.fields.emplace_back(std::make_unique<value>(int_value{0}));
    v2.fields.emplace_back(std::make_unique<value>(boolean_value{true}));
    ASSERT_NE(v1, v2);

    struct_value v3;
    v3.fields.emplace_back(std::make_unique<value>(int_value{1}));
    v3.fields.emplace_back(std::make_unique<value>(boolean_value{false}));
    ASSERT_NE(v1, v3);

    struct_value v4;
    v4.fields.emplace_back(std::make_unique<value>(int_value{0}));
    ASSERT_NE(v1, v4);

    struct_value v5;
    v5.fields.emplace_back(std::make_unique<value>(boolean_value{false}));
    ASSERT_NE(v1, v5);

    struct_value v6;
    v6.fields.emplace_back(std::make_unique<value>(int_value{0}));
    v6.fields.emplace_back(nullptr);
    ASSERT_NE(v1, v6);

    struct_value v1_nested;
    v1_nested.fields.emplace_back(std::make_unique<value>(std::move(v1_copy)));
    ASSERT_EQ(v1, std::get<struct_value>(*v1_nested.fields[0]));
    ASSERT_NE(v1, v1_nested);
    ASSERT_NE(v1, v1_copy);

    struct_value v_null;
    v_null.fields.emplace_back(nullptr);
    v_null.fields.emplace_back(nullptr);
    ASSERT_NE(v1, v_null);
    ASSERT_EQ(v_null, v_null);
}

TEST(ValuesTest, TestListEquality) {
    list_value v1;
    v1.elements.emplace_back(std::make_unique<value>(int_value{0}));
    v1.elements.emplace_back(std::make_unique<value>(int_value{1}));
    ASSERT_EQ(v1, v1);

    list_value v1_copy;
    v1_copy.elements.emplace_back(std::make_unique<value>(int_value{0}));
    v1_copy.elements.emplace_back(std::make_unique<value>(int_value{1}));
    ASSERT_EQ(v1, v1_copy);

    list_value v2;
    v2.elements.emplace_back(std::make_unique<value>(int_value{0}));
    v2.elements.emplace_back(std::make_unique<value>(int_value{2}));
    ASSERT_NE(v1, v2);

    list_value v3;
    v3.elements.emplace_back(std::make_unique<value>(int_value{2}));
    v3.elements.emplace_back(std::make_unique<value>(int_value{0}));
    ASSERT_NE(v1, v3);

    list_value v4;
    v4.elements.emplace_back(std::make_unique<value>(int_value{0}));
    ASSERT_NE(v1, v4);

    list_value v5;
    v5.elements.emplace_back(std::make_unique<value>(int_value{0}));
    v5.elements.emplace_back(nullptr);
    ASSERT_NE(v1, v5);

    list_value v1_nested;
    v1_nested.elements.emplace_back(
      std::make_unique<value>(std::move(v1_copy)));
    ASSERT_EQ(v1, std::get<list_value>(*v1_nested.elements[0]));
    ASSERT_NE(v1, v1_nested);
    ASSERT_NE(v1, v1_copy);

    list_value v_null;
    v_null.elements.emplace_back(nullptr);
    v_null.elements.emplace_back(nullptr);
    ASSERT_NE(v1, v_null);
    ASSERT_EQ(v_null, v_null);
}
