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

#include "bytes/iobuf.h"
#include "serde/parquet/variant_value.h"

namespace serde::parquet {

struct encoded_variant {
    iobuf metadata;
    iobuf value;
};

encoded_variant encode_variant(const variant_value& val);
variant_value decode_variant(const iobuf& metadata, const iobuf& value);

} // namespace serde::parquet
