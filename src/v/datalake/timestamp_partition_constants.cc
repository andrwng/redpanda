// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "datalake/timestamp_partition_constants.h"

#include "iceberg/datatypes.h"

namespace datalake {

iceberg::nested_field_ptr make_timestamp_field() {
    return iceberg::nested_field::create(
      timestamp_field_id(),
      ss::sstring{timestamp_field_name},
      iceberg::field_required::yes,
      iceberg::timestamp_type());
}

} // namespace datalake
