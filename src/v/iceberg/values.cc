// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/values.h"

#include "bytes/hash.h"
#include "iceberg/datatypes.h"

#include <boost/container_hash/hash_fwd.hpp>

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

namespace {

struct primitive_hashing_visitor {
    size_t operator()(const boolean_value& v) const {
        return std::hash<bool>()(v.val);
    }
    size_t operator()(const int_value& v) const {
        return std::hash<int>()(v.val);
    }
    size_t operator()(const long_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const float_value& v) const {
        return std::hash<float>()(v.val);
    }
    size_t operator()(const double_value& v) const {
        return std::hash<double>()(v.val);
    }
    size_t operator()(const date_value& v) const {
        return std::hash<int32_t>()(v.val);
    }
    size_t operator()(const time_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const timestamp_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const timestamptz_value& v) const {
        return std::hash<int64_t>()(v.val);
    }
    size_t operator()(const string_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const uuid_value& v) const {
        return absl::Hash<uuid_t>()(v.val);
    }
    size_t operator()(const fixed_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const binary_value& v) const {
        return std::hash<iobuf>()(v.val);
    }
    size_t operator()(const decimal_value& v) const {
        return absl::Hash<absl::int128>()(v.val);
    }
};

struct hashing_visitor {
    size_t operator()(const primitive_value& v) const {
        return std::visit(primitive_hashing_visitor{}, v);
    }

    size_t operator()(const struct_value& v) const {
        size_t h = 0;
        for (const auto& f : v.fields) {
            if (!f) {
                continue;
            }
            boost::hash_combine(h, std::hash<value>()(*f));
        }
        return h;
    }
    size_t operator()(const list_value& v) const {
        size_t h = 0;
        for (const auto& e : v.elements) {
            if (!e) {
                continue;
            }
            boost::hash_combine(h, std::hash<value>()(*e));
        }
        return h;
    }
    size_t operator()(const map_value& v) const {
        size_t h = 0;
        for (const auto& kv : v.kvs) {
            if (kv.key) {
                boost::hash_combine(h, std::hash<value>()(*kv.key));
            }
            if (kv.val) {
                boost::hash_combine(h, std::hash<value>()(*kv.val));
            }
        }
        return h;
    }
};

} // namespace

size_t value_hash(const struct_value& v) {
    size_t h = 0;
    for (const auto& f : v.fields) {
        if (!f) {
            continue;
        }
        boost::hash_combine(h, std::hash<value>()(*f));
    }
    return h;
}

size_t value_hash(const value& v) { return std::visit(hashing_visitor{}, v); }

} // namespace iceberg
