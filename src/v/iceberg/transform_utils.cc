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

struct transform_applying_visitor {
    explicit transform_applying_visitor(const value& source_val)
      : source_val_(source_val) {}
    const value& source_val_;

    value_ptr operator()(const day_transform&) {
        // TODO: implement me! And everything else.
        int_value v{0};
        return std::make_unique<value>(v);
    }

    template<typename T>
    value_ptr operator()(const T&) {
        throw std::invalid_argument("Not supported");
    }
};

value_ptr apply_transform(const value& source_val, const transform& transform) {
    return std::visit(transform_applying_visitor{source_val}, transform);
}

} // namespace iceberg
