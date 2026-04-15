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

#include "serde/parquet/variant_shredder.h"

#include "bytes/iobuf.h"

#include <algorithm>

namespace serde::parquet {

namespace {

/// Navigate a field_path through the variant_value tree, returning a pointer
/// to the leaf variant_value if the path is valid, or nullptr if any step
/// fails (not an object or key not found).
variant_value*
navigate_path(variant_value& root, const chunked_vector<ss::sstring>& path) {
    variant_value* current = &root;
    for (const auto& key : path) {
        auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(current);
        if (!obj_ptr || !*obj_ptr) {
            return nullptr;
        }
        auto& fields = (*obj_ptr)->fields;
        auto it = std::ranges::find_if(
          fields, [&key](const auto& p) { return p.first == key; });
        if (it == fields.end()) {
            return nullptr;
        }
        current = &it->second;
    }
    return current;
}

/// Check whether a variant_value's runtime type matches the requested
/// physical_type, and if so convert it to a parquet value.
std::optional<value>
try_extract(const variant_value& vval, const physical_type& pt) {
    return std::visit(
      [&vval](const auto& type_tag) -> std::optional<value> {
          using T = std::decay_t<decltype(type_tag)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, bool_type>) {
              if (auto* v = std::get_if<boolean_value>(&vval)) {
                  return value{*v};
              }
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, i32_type>) {
              if (auto* v = std::get_if<int32_value>(&vval)) {
                  return value{*v};
              }
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, i64_type>) {
              if (auto* v = std::get_if<int64_value>(&vval)) {
                  return value{*v};
              }
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, f32_type>) {
              if (auto* v = std::get_if<float32_value>(&vval)) {
                  return value{*v};
              }
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, f64_type>) {
              if (auto* v = std::get_if<float64_value>(&vval)) {
                  return value{*v};
              }
              return std::nullopt;
          } else if constexpr (std::is_same_v<T, byte_array_type>) {
              if (auto* v = std::get_if<variant_string_value>(&vval)) {
                  return value{byte_array_value{iobuf::from(v->val)}};
              }
              if (auto* v = std::get_if<byte_array_value>(&vval)) {
                  return value{byte_array_value{v->val.copy()}};
              }
              return std::nullopt;
          } else {
              return std::nullopt;
          }
      },
      pt);
}

/// Remove a field at the given path from the variant_value tree. Navigates
/// to the parent object and erases the final key.
void remove_field(
  variant_value& root, const chunked_vector<ss::sstring>& path) {
    if (path.empty()) {
        return;
    }
    variant_value* current = &root;
    for (size_t i = 0; i < path.size() - 1; ++i) {
        auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(current);
        if (!obj_ptr || !*obj_ptr) {
            return;
        }
        auto& fields = (*obj_ptr)->fields;
        auto it = std::ranges::find_if(
          fields, [&key = path[i]](const auto& p) { return p.first == key; });
        if (it == fields.end()) {
            return;
        }
        current = &it->second;
    }
    auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(current);
    if (!obj_ptr || !*obj_ptr) {
        return;
    }
    auto& fields = (*obj_ptr)->fields;
    const auto& last_key = path.back();
    // chunked_vector lacks iterator erase, so find the index and swap-remove.
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].first == last_key) {
            if (i + 1 < fields.size()) {
                std::swap(fields[i], fields[fields.size() - 1]);
            }
            fields.pop_back();
            break;
        }
    }
}

} // namespace

shredded_variant
shred_variant(variant_value val, const variant_shredding_schema& schema) {
    shredded_variant result;
    result.typed_values.reserve(schema.fields.size());

    for (const auto& field : schema.fields) {
        auto* leaf = navigate_path(val, field.field_path);
        if (!leaf) {
            result.typed_values.emplace_back(null_value{});
            continue;
        }
        auto extracted = try_extract(*leaf, field.type);
        if (!extracted) {
            result.typed_values.emplace_back(null_value{});
            continue;
        }
        result.typed_values.emplace_back(std::move(*extracted));
        remove_field(val, field.field_path);
    }

    result.residual = std::move(val);
    return result;
}

} // namespace serde::parquet
