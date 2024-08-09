// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/partition.h"
#include "iceberg/transform.h"

namespace datalake {

static constexpr std::string_view timestamp_partition_field_name
  = "redpanda_hour";
static constexpr std::string_view timestamp_field_name
  = "redpanda_timestamp_micros";
static constexpr iceberg::nested_field::id_t timestamp_field_id{0};
// Partition field ID assignment starts at 1000 in reference implementations.
// TODO: may need to adjust, based on what's in the catalog, if we ever support
// pointing to arbitrary tables.
static constexpr iceberg::partition_field::id_t timestamp_partition_field_id{
  1000};
static const iceberg::partition_field timestamp_partition_field{
  .source_id = timestamp_field_id,
  .field_id = timestamp_partition_field_id,
  .name = ss::sstring{timestamp_partition_field_name},
  .transform = iceberg::hour_transform(),
};
iceberg::nested_field_ptr make_timestamp_field();

} // namespace datalake
