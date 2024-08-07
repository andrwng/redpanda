// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/schema_avro.h"

#include "iceberg/schema.h"

#include <avro/CustomAttributes.hh>
#include <avro/LogicalType.hh>
#include <avro/Schema.hh>
#include <fmt/core.h>

#include <stdexcept>
#include <variant>

namespace iceberg {

namespace {

avro::CustomAttributes field_attrs(int field_id) {
    avro::CustomAttributes attrs;
    attrs.addAttribute("field-id", fmt::format("{}", field_id));
    return attrs;
}

avro::Schema optional_of(const avro::Schema& type) {
    avro::UnionSchema s;
    s.addType(avro::NullSchema());
    s.addType(type);
    return s;
}

avro::Schema map_of(const avro::Schema& kv_type) {
    // Map types are represented as an array of key-value pairs with a logical
    // type of "map".
    avro::ArraySchema schema(kv_type);
    schema.root()->setLogicalType(avro::LogicalType(avro::LogicalType::MAP));
    return schema;
}

avro::Schema array_of(const avro::Schema& type, int element_id) {
    avro::ArraySchema schema(type, element_id);
    return schema;
}

avro::Schema kv_schema(
  const std::string& name,
  int key_id,
  const avro::Schema& key_schema,
  int val_id,
  const avro::Schema& val_schema) {
    avro::RecordSchema schema(name);
    schema.addField("key", key_schema, field_attrs(key_id));
    schema.addField("value", val_schema, field_attrs(val_id));
    return schema;
}

// Returns an avro::Schema for the given field, ignoring top-level optionality
// of field. To consider optionality of the top-level field, use
// field_as_avro().
avro::Schema base_field_as_avro(const nested_field& field) {
    const auto& type = field.type;
    if (std::holds_alternative<primitive_type>(type)) {
        const auto& type_as_primitive = std::get<primitive_type>(type);
        if (std::holds_alternative<boolean_type>(type_as_primitive)) {
            return avro::BoolSchema();
        } else if (std::holds_alternative<int_type>(type_as_primitive)) {
            return avro::IntSchema();
        } else if (std::holds_alternative<long_type>(type_as_primitive)) {
            return avro::LongSchema();
        } else if (std::holds_alternative<float_type>(type_as_primitive)) {
            return avro::FloatSchema();
        } else if (std::holds_alternative<double_type>(type_as_primitive)) {
            return avro::DoubleSchema();
        } else if (std::holds_alternative<decimal_type>(type_as_primitive)) {
            const auto& type_as_decimal = std::get<decimal_type>(
              type_as_primitive);
            auto bytes_for_p = static_cast<int>(
              std::ceil((type_as_decimal.precision * std::log2(10) + 1) / 8));
            // XXX: what name should we use instead? Or make FixedSchema
            // constructor not take a name.
            auto ret = avro::FixedSchema(bytes_for_p, "");
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::DECIMAL));
            return ret;
        } else if (std::holds_alternative<date_type>(type_as_primitive)) {
            auto ret = avro::LongSchema();
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::DATE));
            return ret;
        } else if (std::holds_alternative<time_type>(type_as_primitive)) {
            auto ret = avro::LongSchema();
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::TIME_MICROS));
            return ret;
        } else if (std::holds_alternative<timestamp_type>(type_as_primitive)) {
            auto ret = avro::LongSchema();
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
            return ret;
        } else if (std::holds_alternative<timestamptz_type>(
                     type_as_primitive)) {
            auto ret = avro::LongSchema();
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS));
            return ret;
        } else if (std::holds_alternative<string_type>(type_as_primitive)) {
            return avro::StringSchema();
        } else if (std::holds_alternative<uuid_type>(type_as_primitive)) {
            // XXX: what name should we use instead? Or make FixedSchema
            // constructor not take a name.
            auto ret = avro::FixedSchema(16, "");
            ret.root()->setLogicalType(
              avro::LogicalType(avro::LogicalType::UUID));
            return ret;
        } else if (std::holds_alternative<fixed_type>(type_as_primitive)) {
            const auto& type_as_fixed = std::get<fixed_type>(type_as_primitive);
            // XXX: bounds check on length?
            // XXX: what name should we use instead? Or make FixedSchema
            // constructor not take a name.
            auto ret = avro::FixedSchema(
              static_cast<int>(type_as_fixed.length), "");
            return ret;
        } else if (std::holds_alternative<binary_type>(type_as_primitive)) {
            return avro::BytesSchema();
        }
        throw std::invalid_argument(
          fmt::format("Unknown primitive type: {}", type));
    }
    if (std::holds_alternative<struct_type>(type)) {
        const auto& type_as_struct = std::get<struct_type>(type);
        avro::RecordSchema ret(fmt::format("r{}", field.id));
        for (const auto& child_field_ptr : type_as_struct.fields) {
            ret.addField(
              child_field_ptr->name,
              field_as_avro(*child_field_ptr),
              field_attrs(child_field_ptr->id()));
        }
        return ret;
    }
    if (std::holds_alternative<list_type>(type)) {
        const auto& type_as_list = std::get<list_type>(type);
        return array_of(
          field_as_avro(*type_as_list.element_field),
          type_as_list.element_field->id);
    }
    if (std::holds_alternative<map_type>(type)) {
        const auto& type_as_map = std::get<map_type>(type);
        auto kv = kv_schema(
          fmt::format(
            "k{}_v{}",
            type_as_map.key_field->id(),
            type_as_map.value_field->id()),
          type_as_map.key_field->id(),
          field_as_avro(*type_as_map.key_field),
          type_as_map.value_field->id(),
          field_as_avro(*type_as_map.value_field));
        return map_of(kv);
    }
    throw std::invalid_argument(fmt::format("Unknown field type: {}", type));
}

} // namespace

avro::Schema field_as_avro(const nested_field& field) {
    if (field.required) {
        return base_field_as_avro(field);
    }
    return optional_of(base_field_as_avro(field));
}

avro::Schema schema_as_avro(const struct_type& type, const ss::sstring& name) {
    avro::RecordSchema avro_schema(name);
    const auto& fields = type.fields;
    for (const auto& field_ptr : fields) {
        avro_schema.addField(
          field_ptr->name,
          field_as_avro(*field_ptr),
          field_attrs(field_ptr->id()));
    }
    return avro_schema;
}

} // namespace iceberg
