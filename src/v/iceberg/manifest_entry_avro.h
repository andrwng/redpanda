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

#include <avro/Schema.hh>

namespace iceberg {

avro::Schema manifest_entry_schema(const struct_type& partition_type);
struct_type manifest_entry_type(struct_type partition_type);

} // namespace iceberg
