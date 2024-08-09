// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "datalake/logger.h"
#include "datalake/timestamp_partition_constants.h"
#include "iceberg/partition.h"
#include "iceberg/partition_key.h"
#include "iceberg/struct_accessor.h"
#include "iceberg/values.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

using namespace datalake;

iceberg::schema make_schema() {
    iceberg::schema schema;
    schema.schema_id = iceberg::schema::id_t{0};
    schema.schema_struct.fields.emplace_back(make_timestamp_field());
    return schema;
}

iceberg::partition_spec make_partition_spec() {
    iceberg::partition_spec spec;
    spec.spec_id = iceberg::partition_spec::id_t{0};
    spec.fields.emplace_back(timestamp_partition_field);
    return spec;
}

iceberg::struct_value val_with_timestamp(model::timestamp timestamp_ms) {
    static constexpr auto micros_per_ms = 1000;
    int64_t timestamp_us = timestamp_ms.value() * micros_per_ms;
    iceberg::struct_value ret;
    ret.fields.emplace_back(
      std::make_unique<iceberg::value>(iceberg::timestamp_value{timestamp_us}));
    return ret;
}

TEST(TimestampPartitionKeyTest, TestHourlyGrouping) {
    const auto timestamp_schema = make_schema();
    const auto timestamp_spec = make_partition_spec();
    const auto accessors = iceberg::struct_accessor::from_struct_type(
      timestamp_schema.schema_struct);
    chunked_vector<iceberg::struct_value> source_vals;
    static constexpr auto ms_per_hour = 3600 * 1000;
    static constexpr auto num_hours = 10;
    static constexpr auto num_per_hour = 5;
    const auto start_time = model::timestamp::now();
    for (int h = 0; h < num_hours; h++) {
        for (int i = 0; i < num_per_hour; i++) {
            source_vals.emplace_back(val_with_timestamp(
              model::timestamp{start_time.value() + h * ms_per_hour + i}));
        }
    }
    chunked_hash_map<
      iceberg::partition_key,
      chunked_vector<iceberg::struct_value>>
      vals_by_key;
    for (auto& v : source_vals) {
        auto pk = iceberg::partition_key::create(v, accessors, timestamp_spec);
        auto [iter, _] = vals_by_key.emplace(
          std::move(pk), chunked_vector<iceberg::struct_value>{});
        iter->second.emplace_back(std::move(v));
    }
    size_t num_vals = 0;
    for (const auto& [pk, vs] : vals_by_key) {
        num_vals += vs.size();
    }
    ASSERT_EQ(vals_by_key.size(), 10);
    ASSERT_EQ(num_vals, num_hours * num_per_hour);
}
