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

#include "serde/parquet/variant_schema.h"

#include "serde/parquet/variant_encoding.h"

#include <algorithm>

namespace serde::parquet {

namespace {

logical_type logical_type_for_shredded_field(const physical_type& pt) {
    if (std::holds_alternative<byte_array_type>(pt)) {
        return string_type{};
    }
    return {};
}

/// Insert a shredding field into a typed_value group schema, creating
/// intermediate group nodes for nested paths as needed.
void insert_shredded_field(
  schema_element& parent,
  const chunked_vector<ss::sstring>& path,
  size_t depth,
  const physical_type& pt) {
    if (depth + 1 == path.size()) {
        parent.children.push_back(
          schema_element{
            .type = pt,
            .repetition_type = field_repetition_type::optional,
            .path = {ss::sstring(path[depth])},
            .logical_type = logical_type_for_shredded_field(pt),
          });
        return;
    }
    const auto& segment = path[depth];
    auto it = std::ranges::find_if(
      parent.children,
      [&segment](const schema_element& e) { return e.name() == segment; });
    if (it == parent.children.end()) {
        parent.children.push_back(
          schema_element{
            .type = std::monostate{},
            .repetition_type = field_repetition_type::optional,
            .path = {ss::sstring(segment)},
          });
        it = parent.children.end() - 1;
    }
    insert_shredded_field(*it, path, depth + 1, pt);
}

/// Build the typed_value group members matching the schema, recursively.
/// Each shredding field's value maps to a leaf; intermediate groups become
/// group_value nodes. The `typed_values` vector is consumed in order,
/// with `idx` tracking which field we are on.
void build_typed_value_members(
  group_value& out,
  const schema_element& schema_node,
  chunked_vector<value>& typed_values,
  size_t& idx) {
    for (const auto& child : schema_node.children) {
        if (child.is_leaf()) {
            out.push_back(group_member{std::move(typed_values[idx++])});
        } else {
            group_value nested;
            build_typed_value_members(nested, child, typed_values, idx);
            out.push_back(group_member{value{std::move(nested)}});
        }
    }
}

bool is_residual_trivial(const variant_value& residual) {
    if (auto* obj = std::get_if<std::unique_ptr<variant_object>>(&residual)) {
        return *obj && (*obj)->fields.empty();
    }
    return false;
}

} // namespace

schema_element build_variant_schema(
  ss::sstring name,
  field_repetition_type rep,
  const variant_shredding_schema& shredding) {
    bool has_shredding = !shredding.fields.empty();

    schema_element root{
      .type = std::monostate{},
      .repetition_type = rep,
      .path = {std::move(name)},
      .logical_type = variant_type{},
    };

    root.children.push_back(
      schema_element{
        .type = byte_array_type{},
        .repetition_type = field_repetition_type::required,
        .path = {ss::sstring("metadata")},
      });

    root.children.push_back(
      schema_element{
        .type = byte_array_type{},
        .repetition_type = has_shredding ? field_repetition_type::optional
                                         : field_repetition_type::required,
        .path = {ss::sstring("value")},
      });

    if (has_shredding) {
        schema_element typed_value{
          .type = std::monostate{},
          .repetition_type = field_repetition_type::optional,
          .path = {ss::sstring("typed_value")},
        };
        for (const auto& field : shredding.fields) {
            insert_shredded_field(typed_value, field.field_path, 0, field.type);
        }
        root.children.push_back(std::move(typed_value));
    }

    return root;
}

group_value encode_variant_for_writer(
  variant_value val, const variant_shredding_schema& shredding) {
    if (shredding.fields.empty()) {
        auto enc = encode_variant(val);
        group_value gv;
        gv.push_back(
          group_member{value{byte_array_value{std::move(enc.metadata)}}});
        gv.push_back(
          group_member{value{byte_array_value{std::move(enc.value)}}});
        return gv;
    }

    auto shredded = shred_variant(std::move(val), shredding);
    auto enc = encode_variant(shredded.residual);

    group_value gv;
    gv.push_back(
      group_member{value{byte_array_value{std::move(enc.metadata)}}});

    if (is_residual_trivial(shredded.residual)) {
        gv.push_back(group_member{value{null_value{}}});
    } else {
        gv.push_back(
          group_member{value{byte_array_value{std::move(enc.value)}}});
    }

    // Build typed_value group matching the schema structure.
    auto typed_value_schema = build_variant_schema("tmp", {}, shredding);
    // typed_value is the third child (index 2).
    const auto& tv_schema = typed_value_schema.children[2];

    group_value tv;
    size_t idx = 0;
    build_typed_value_members(tv, tv_schema, shredded.typed_values, idx);
    gv.push_back(group_member{value{std::move(tv)}});

    return gv;
}

namespace {

/// Convert a parquet value read from a typed_value leaf back to a
/// variant_value, reversing the conversion done by try_extract in the
/// shredder.
variant_value
parquet_value_to_variant(const value& v, const physical_type& pt) {
    if (std::holds_alternative<null_value>(v)) {
        return null_value{};
    }
    if (std::holds_alternative<boolean_value>(v)) {
        return std::get<boolean_value>(v);
    }
    if (std::holds_alternative<int32_value>(v)) {
        return std::get<int32_value>(v);
    }
    if (std::holds_alternative<int64_value>(v)) {
        return std::get<int64_value>(v);
    }
    if (std::holds_alternative<float32_value>(v)) {
        return std::get<float32_value>(v);
    }
    if (std::holds_alternative<float64_value>(v)) {
        return std::get<float64_value>(v);
    }
    if (auto* ba = std::get_if<byte_array_value>(&v)) {
        if (std::holds_alternative<byte_array_type>(pt)) {
            // The shredder encodes variant_string_value as byte_array_value,
            // so reverse that here.
            return variant_string_value{ba->val.linearize_to_string()};
        }
        return byte_array_value{ba->val.copy()};
    }
    return null_value{};
}

/// Insert a variant_value at the given field_path within a variant_object,
/// creating intermediate objects as needed.
void insert_at_path(
  variant_object& root,
  const chunked_vector<ss::sstring>& path,
  variant_value leaf_val) {
    variant_object* current = &root;
    for (size_t i = 0; i + 1 < path.size(); ++i) {
        const auto& key = path[i];
        auto it = std::ranges::find_if(
          current->fields, [&key](const auto& p) { return p.first == key; });
        if (it == current->fields.end()) {
            auto nested = std::make_unique<variant_object>();
            current->fields.emplace_back(
              ss::sstring(key), variant_value{std::move(nested)});
            auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(
              &current->fields.back().second);
            current = obj_ptr->get();
        } else {
            auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(
              &it->second);
            if (!obj_ptr || !*obj_ptr) {
                it->second = variant_value{std::make_unique<variant_object>()};
                obj_ptr = std::get_if<std::unique_ptr<variant_object>>(
                  &it->second);
            }
            current = obj_ptr->get();
        }
    }
    current->fields.emplace_back(ss::sstring(path.back()), std::move(leaf_val));
}

/// Recursively sort all variant_objects in the tree.
void sort_variant_object_recursive(variant_value& val) {
    auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(&val);
    if (!obj_ptr || !*obj_ptr) {
        return;
    }
    for (auto& [_, child] : (*obj_ptr)->fields) {
        sort_variant_object_recursive(child);
    }
    sort_variant_object(*obj_ptr->get());
}

size_t count_leaf_descendants(const schema_element& e) {
    if (e.is_leaf()) {
        return 1;
    }
    size_t count = 0;
    for (const auto& c : e.children) {
        count += count_leaf_descendants(c);
    }
    return count;
}

/// Extract typed leaf values from the typed_value group, navigating nested
/// groups to match the schema structure built by build_variant_schema.
void extract_typed_values_from_group(
  const group_value& group,
  const schema_element& schema_node,
  const chunked_vector<variant_shredding_field>& fields,
  size_t& field_idx,
  variant_object& target) {
    size_t child_idx = 0;
    for (const auto& schema_child : schema_node.children) {
        if (child_idx >= group.size()) {
            break;
        }
        const auto& member = group[child_idx];
        if (schema_child.is_leaf()) {
            if (field_idx < fields.size()) {
                const auto& field = fields[field_idx];
                if (!std::holds_alternative<null_value>(member.field)) {
                    auto vval = parquet_value_to_variant(
                      member.field, field.type);
                    if (!std::holds_alternative<null_value>(vval)) {
                        insert_at_path(
                          target, field.field_path, std::move(vval));
                    }
                }
                ++field_idx;
            }
        } else {
            const auto* nested = std::get_if<group_value>(&member.field);
            if (nested) {
                extract_typed_values_from_group(
                  *nested, schema_child, fields, field_idx, target);
            } else {
                field_idx += count_leaf_descendants(schema_child);
            }
        }
        ++child_idx;
    }
}

} // unnamed namespace

variant_value decode_variant_from_reader(
  const group_value& variant_group, const variant_shredding_schema& shredding) {
    if (shredding.fields.empty()) {
        // Unshredded: group has metadata + value.
        const auto& meta_ba = std::get<byte_array_value>(
          variant_group[0].field);
        const auto& val_ba = std::get<byte_array_value>(variant_group[1].field);
        return decode_variant(meta_ba.val, val_ba.val);
    }

    // Shredded: group has metadata + value (possibly null) + typed_value.
    const auto& meta_ba = std::get<byte_array_value>(variant_group[0].field);

    variant_value result;
    if (auto* val_ba = std::get_if<byte_array_value>(&variant_group[1].field)) {
        result = decode_variant(meta_ba.val, val_ba->val);
    } else {
        result = variant_value{std::make_unique<variant_object>()};
    }

    const auto* typed_group = std::get_if<group_value>(&variant_group[2].field);
    if (typed_group) {
        auto tv_schema = build_variant_schema("tmp", {}, shredding);
        const auto& tv_schema_node = tv_schema.children[2];

        auto* obj_ptr = std::get_if<std::unique_ptr<variant_object>>(&result);
        if (obj_ptr && *obj_ptr) {
            size_t field_idx = 0;
            extract_typed_values_from_group(
              *typed_group,
              tv_schema_node,
              shredding.fields,
              field_idx,
              **obj_ptr);
        }
    }

    sort_variant_object_recursive(result);
    return result;
}

} // namespace serde::parquet
