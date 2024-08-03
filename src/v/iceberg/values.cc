// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/values.h"

#include "iceberg/datatypes.h"

namespace iceberg {

struct primitive_value_comparison_visitor {
    template<typename T, typename U>
    bool operator()(const T&, const U&) const {
        static_assert(!std::is_same<T, U>::value);
        return false;
    }
    template<typename T>
    requires requires(T t) { t.val; }
    bool operator()(const T& lhs, const T& rhs) const {
        return lhs.val == rhs.val;
    }
};

bool operator==(const primitive_value& lhs, const primitive_value& rhs) {
    return std::visit(primitive_value_comparison_visitor{}, lhs, rhs);
}

bool operator==(const struct_value& lhs, const struct_value& rhs) {
    if (lhs.fields.size() != rhs.fields.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.fields.size(); i++) {
        auto has_lhs = lhs.fields[i] != nullptr;
        auto has_rhs = rhs.fields[i] != nullptr;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (!has_lhs) {
            // Both are null.
            continue;
        }
        if (*lhs.fields[i] != *rhs.fields[i]) {
            return false;
        }
    }
    return true;
}

bool operator==(const list_value& lhs, const list_value& rhs) {
    if (lhs.elements.size() != rhs.elements.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.elements.size(); i++) {
        auto has_lhs = lhs.elements[i] != nullptr;
        auto has_rhs = rhs.elements[i] != nullptr;
        if (has_lhs != has_rhs) {
            return false;
        }
        if (!has_lhs) {
            // Both are null.
            continue;
        }
        if (*lhs.elements[i] != *rhs.elements[i]) {
            return false;
        }
    }
    return true;
}

bool operator==(const kv_value& lhs, const kv_value& rhs) {
    auto has_lhs_key = lhs.key != nullptr;
    auto has_rhs_key = rhs.key != nullptr;
    if (has_lhs_key != has_rhs_key) {
        return false;
    }
    auto has_lhs_val = lhs.val != nullptr;
    auto has_rhs_val = rhs.val != nullptr;
    if (has_lhs_val != has_rhs_val) {
        return false;
    }
    if (has_lhs_key && *lhs.key != *rhs.key) {
        return false;
    }
    if (has_lhs_val && *lhs.val != *rhs.val) {
        return false;
    }
    return true;
}

bool operator==(const map_value& lhs, const map_value& rhs) {
    if (lhs.kvs.size() != rhs.kvs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.kvs.size(); i++) {
        if (lhs.kvs[i] != rhs.kvs[i]) {
            return false;
        }
    }
    return true;
}

} // namespace iceberg
