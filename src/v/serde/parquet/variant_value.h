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
#include "container/chunked_vector.h"
#include "serde/parquet/value.h"

#include <seastar/core/sstring.hh>

#include <memory>
#include <utility>
#include <variant>

namespace serde::parquet {

struct variant_object;
struct variant_array;

struct variant_string_value {
    ss::sstring val;
    bool operator==(const variant_string_value&) const = default;
};

/// In-memory representation of a Parquet VARIANT value.
///
/// A recursive typed value tree with objects, arrays, and primitives.
/// Used as the common intermediate between input adapters (JSON, etc.)
/// and the variant binary encoder/shredder.
using variant_value = std::variant<
  null_value,
  boolean_value,
  int32_value,
  int64_value,
  float32_value,
  float64_value,
  byte_array_value,
  variant_string_value,
  std::unique_ptr<variant_object>,
  std::unique_ptr<variant_array>>;

bool operator==(const variant_value& lhs, const variant_value& rhs);

/// A variant object with string-keyed fields, sorted by key for
/// deterministic encoding.
struct variant_object {
    chunked_vector<std::pair<ss::sstring, variant_value>> fields;
    bool operator==(const variant_object&) const;
};

/// A variant array of heterogeneous values.
struct variant_array {
    chunked_vector<variant_value> elements;
    bool operator==(const variant_array&) const;
};

/// Deep copy a variant_value (unique_ptr members are not copyable).
variant_value copy_variant(const variant_value& val);

/// Sort a variant_object's fields by key lexicographically.
void sort_variant_object(variant_object& obj);

} // namespace serde::parquet
