// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/transform_utils.h"

#include "iceberg/datatypes.h"
#include "iceberg/transform.h"

namespace iceberg {

// TODO: at some point we should restrict the source types to what is defined
// by the spec. For now, expect that the input types are allowed.
struct transform_result_type_visitor {
    explicit transform_result_type_visitor(const field_type& source)
      : source_type_(source) {}

    const field_type& source_type_;

    field_type operator()(const identity_transform&) {
        return make_copy(source_type_);
    }
    field_type operator()(const bucket_transform&) { return int_type(); }
    field_type operator()(const truncate_transform&) {
        return make_copy(source_type_);
    }
    field_type operator()(const year_transform&) { return int_type(); }
    field_type operator()(const month_transform&) { return int_type(); }
    field_type operator()(const day_transform&) { return int_type(); }
    field_type operator()(const hour_transform&) { return int_type(); }
    field_type operator()(const void_transform&) {
        // TODO: the spec also says the result may also be the source type.
        return int_type();
    }
};

field_type
get_result_type(const field_type& source_type, const transform& transform) {
    return std::visit(transform_result_type_visitor{source_type}, transform);
}

namespace {

static constexpr double hr_per_sec = 1.0 / 3600.0;
int32_t micros_to_hr(int64_t micros) {
    return static_cast<int32_t>(
      static_cast<double>(micros) / 1000.0 / 1000.0 * hr_per_sec);
}

struct hourly_visitor {
    int32_t operator()(const primitive_value& v) {
        if (std::holds_alternative<timestamp_value>(v)) {
            return micros_to_hr(std::get<timestamp_value>(v).val);
        }
        throw std::invalid_argument(fmt::format(
          "hourly_visitor not implemented for primitive value {}", v));
    }

    template<typename T>
    int32_t operator()(const T& t) {
        throw std::invalid_argument(
          fmt::format("hourly_visitor not implemented for value {}", t));
    }
};

} // namespace

struct transform_applying_visitor {
    explicit transform_applying_visitor(const value& source_val)
      : source_val_(source_val) {}
    const value& source_val_;

    value_ptr operator()(const hour_transform&) {
        // TODO: implement me! And everything else.
        int_value v{std::visit(hourly_visitor{}, source_val_)};
        return std::make_unique<value>(v);
    }

    template<typename T>
    value_ptr operator()(const T&) {
        throw std::invalid_argument(
          "transform_applying_visitor not implemented for transform");
    }
};

value_ptr apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

} // namespace iceberg
