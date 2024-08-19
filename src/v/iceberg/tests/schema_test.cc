// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema.h"
#include "iceberg/tests/test_schemas.h"

#include <gtest/gtest.h>

using namespace iceberg;

TEST(SchemaTest, TestGetTypesNestedSchema) {
    schema s{
      .schema_struct = std::get<struct_type>(test_nested_schema_type()),
      .schema_id = schema::id_t{0},
      .identifier_field_ids = {},
    };
    const auto ids_to_types = s.ids_to_types();
    ASSERT_EQ(17, ids_to_types.size());
    // First check all ids are accounted for.
    for (int i = 1; i <= 17; i++) {
        ASSERT_TRUE(ids_to_types.contains(nested_field::id_t{i}));
    }
    auto get_primitive_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        EXPECT_TRUE(std::holds_alternative<primitive_type>(*type));
        return std::get<primitive_type>(*type);
    };
    auto check_list_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<list_type>(*type));
    };
    auto check_struct_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<struct_type>(*type));
    };
    auto check_map_type = [&](int32_t id) {
        auto* type = ids_to_types.at(nested_field::id_t{id});
        EXPECT_TRUE(type);
        ASSERT_TRUE(std::holds_alternative<map_type>(*type));
    };
    // Now check the types of each id.
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(1)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(2)));
    ASSERT_TRUE(std::holds_alternative<boolean_type>(get_primitive_type(3)));
    ASSERT_NO_FATAL_FAILURE(check_list_type(4));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(5)));
    ASSERT_NO_FATAL_FAILURE(check_map_type(6));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(7)));
    ASSERT_NO_FATAL_FAILURE(check_map_type(8));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(9)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(10)));
    ASSERT_NO_FATAL_FAILURE(check_list_type(11));
    ASSERT_NO_FATAL_FAILURE(check_struct_type(12));
    ASSERT_TRUE(std::holds_alternative<float_type>(get_primitive_type(13)));
    ASSERT_TRUE(std::holds_alternative<float_type>(get_primitive_type(14)));
    ASSERT_NO_FATAL_FAILURE(check_struct_type(15));
    ASSERT_TRUE(std::holds_alternative<string_type>(get_primitive_type(16)));
    ASSERT_TRUE(std::holds_alternative<int_type>(get_primitive_type(17)));
}
