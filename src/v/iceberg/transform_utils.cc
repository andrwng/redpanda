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

    value operator()(const hour_transform&) {
        // TODO: implement me! And everything else.
        int_value v{std::visit(hourly_visitor{}, source_val_)};
        return v;
    }

    template<typename T>
    value operator()(const T&) {
        throw std::invalid_argument(
          "transform_applying_visitor not implemented for transform");
    }
};

value apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

} // namespace iceberg
