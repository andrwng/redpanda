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

#include "serde/parquet/variant_json.h"

#include "serde/json/parser.h"

#include <seastar/core/coroutine.hh>

#include <fmt/format.h>

#include <cstdint>
#include <limits>
#include <stdexcept>

namespace serde::parquet {

namespace {

ss::sstring iobuf_to_sstring(iobuf buf) {
    ss::sstring str;
    for (const auto& frag : buf) {
        str.append(frag.get(), frag.size());
    }
    return str;
}

ss::future<variant_value> parse_value(serde::json::parser& p) {
    switch (p.token()) {
    case serde::json::token::value_null:
        co_return null_value{};
    case serde::json::token::value_true:
        co_return boolean_value{true};
    case serde::json::token::value_false:
        co_return boolean_value{false};
    case serde::json::token::value_int: {
        auto v = p.value_int();
        if (
          v >= std::numeric_limits<int32_t>::min()
          && v <= std::numeric_limits<int32_t>::max()) {
            co_return int32_value{static_cast<int32_t>(v)};
        }
        co_return int64_value{v};
    }
    case serde::json::token::value_double:
        co_return float64_value{p.value_double()};
    case serde::json::token::value_string:
        co_return variant_string_value{iobuf_to_sstring(p.value_string())};
    case serde::json::token::start_object: {
        auto obj = std::make_unique<variant_object>();
        while (co_await p.next()
               && p.token() != serde::json::token::end_object) {
            auto key = iobuf_to_sstring(p.value_string());
            co_await p.next();
            auto val = co_await parse_value(p);
            obj->fields.emplace_back(std::move(key), std::move(val));
        }
        sort_variant_object(*obj);
        co_return std::move(obj);
    }
    case serde::json::token::start_array: {
        auto arr = std::make_unique<variant_array>();
        while (co_await p.next()
               && p.token() != serde::json::token::end_array) {
            arr->elements.push_back(co_await parse_value(p));
        }
        co_return std::move(arr);
    }
    default:
        throw std::runtime_error(
          fmt::format("unexpected JSON token: {}", p.token()));
    }
}

} // namespace

ss::future<variant_value> parse_json_to_variant(iobuf json_data) {
    serde::json::parser p(std::move(json_data));
    co_await p.next();
    co_return co_await parse_value(p);
}

} // namespace serde::parquet
