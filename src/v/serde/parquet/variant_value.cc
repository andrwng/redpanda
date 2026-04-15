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

#include "serde/parquet/variant_value.h"

#include <algorithm>

namespace serde::parquet {

bool operator==(const variant_value& lhs, const variant_value& rhs) {
    if (lhs.index() != rhs.index()) {
        return false;
    }
    return std::visit(
      [&rhs](const auto& lhs_val) -> bool {
          using T = std::decay_t<decltype(lhs_val)>;
          const auto& rhs_val = std::get<T>(rhs);
          if constexpr (std::is_same_v<T, std::unique_ptr<variant_object>>) {
              if (!lhs_val && !rhs_val) {
                  return true;
              }
              if (!lhs_val || !rhs_val) {
                  return false;
              }
              return *lhs_val == *rhs_val;
          } else if constexpr (
            std::is_same_v<T, std::unique_ptr<variant_array>>) {
              if (!lhs_val && !rhs_val) {
                  return true;
              }
              if (!lhs_val || !rhs_val) {
                  return false;
              }
              return *lhs_val == *rhs_val;
          } else {
              return lhs_val == rhs_val;
          }
      },
      lhs);
}

bool variant_object::operator==(const variant_object& other) const {
    if (fields.size() != other.fields.size()) {
        return false;
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].first != other.fields[i].first) {
            return false;
        }
        if (!(fields[i].second == other.fields[i].second)) {
            return false;
        }
    }
    return true;
}

bool variant_array::operator==(const variant_array& other) const {
    if (elements.size() != other.elements.size()) {
        return false;
    }
    for (size_t i = 0; i < elements.size(); ++i) {
        if (!(elements[i] == other.elements[i])) {
            return false;
        }
    }
    return true;
}

variant_value copy_variant(const variant_value& val) {
    return std::visit(
      [](const auto& v) -> variant_value {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::unique_ptr<variant_object>>) {
              auto obj = std::make_unique<variant_object>();
              for (const auto& [key, field_val] : v->fields) {
                  obj->fields.emplace_back(
                    ss::sstring(key), copy_variant(field_val));
              }
              return obj;
          } else if constexpr (
            std::is_same_v<T, std::unique_ptr<variant_array>>) {
              auto arr = std::make_unique<variant_array>();
              for (const auto& elem : v->elements) {
                  arr->elements.emplace_back(copy_variant(elem));
              }
              return arr;
          } else if constexpr (std::is_same_v<T, byte_array_value>) {
              return byte_array_value{.val = v.val.copy()};
          } else {
              return v;
          }
      },
      val);
}

void sort_variant_object(variant_object& obj) {
    std::ranges::sort(obj.fields, [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
}

} // namespace serde::parquet
