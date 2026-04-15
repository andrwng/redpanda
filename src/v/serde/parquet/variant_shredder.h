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

#pragma once

#include "container/chunked_vector.h"
#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/variant_value.h"

#include <seastar/core/sstring.hh>

namespace serde::parquet {

struct variant_shredding_field {
    chunked_vector<ss::sstring> field_path;
    physical_type type;
};

struct variant_shredding_schema {
    chunked_vector<variant_shredding_field> fields;
};

struct shredded_variant {
    /// One parquet value per field in the shredding schema, in order.
    /// null_value if the field is absent or has wrong type.
    chunked_vector<value> typed_values;

    /// The variant value with shredded fields removed.
    variant_value residual;
};

shredded_variant
shred_variant(variant_value val, const variant_shredding_schema& schema);

} // namespace serde::parquet
