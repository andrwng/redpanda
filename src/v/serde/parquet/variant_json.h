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

#include <seastar/core/future.hh>

namespace serde::parquet {

/// \brief Parse a JSON iobuf into a variant_value.
///
/// Uses serde::json::parser (async streaming). Type inference:
/// integers -> int32/int64, decimals -> float64, strings ->
/// variant_string_value.
ss::future<variant_value> parse_json_to_variant(iobuf json_data);

} // namespace serde::parquet
