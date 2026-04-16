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

#include "serde/parquet/schema.h"
#include "serde/parquet/value.h"
#include "serde/parquet/variant_shredder.h"
#include "serde/parquet/variant_value.h"

namespace serde::parquet {

/// \brief Build a Parquet schema_element for a VARIANT column.
///
/// Without shredding: group with metadata (required BYTE_ARRAY) +
/// value (required BYTE_ARRAY).
///
/// With shredding: group with metadata (required BYTE_ARRAY) +
/// value (optional BYTE_ARRAY) + typed_value (optional group with
/// typed sub-columns for each shredding field).
schema_element build_variant_schema(
  ss::sstring name,
  field_repetition_type rep,
  const variant_shredding_schema& shredding = {});

/// \brief Convert a variant_value into a group_value for the writer.
///
/// Handles shredding, binary encoding of the residual, and
/// construction of the group_value matching the schema produced by
/// build_variant_schema with the same shredding schema.
group_value encode_variant_for_writer(
  variant_value val, const variant_shredding_schema& shredding = {});

/// \brief Decode a variant_value from a group_value produced by the reader.
///
/// For unshredded variants: extracts metadata + value byte arrays and
/// calls decode_variant.
/// For shredded variants: also merges typed values back into the
/// decoded residual (unshredding).
variant_value decode_variant_from_reader(
  const group_value& variant_group,
  const variant_shredding_schema& shredding = {});

} // namespace serde::parquet
