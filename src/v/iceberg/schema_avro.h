// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/schema.h"

#include <avro/Schema.hh>

namespace iceberg {

avro::Schema field_as_avro(const nested_field& field);
avro::Schema schema_as_avro(const struct_type&, const ss::sstring&);

} // namespace iceberg
