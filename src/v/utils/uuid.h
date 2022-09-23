// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include <vector>

#include <absl/hash/hash.h>
#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "bytes/details/out_of_range.h"
#include "reflection/adl.h"
#include "serde/serde.h"
#include "utils/base64.h"

// Wrapper around Boost's UUID suitable for serialization with serde.
// Provides utilities to construct and convert to other types.
struct uuid_t {
public:
    static constexpr auto length = 16;
    using underlying_t = boost::uuids::uuid;

    underlying_t uuid;

    explicit uuid_t(const underlying_t& uuid)
      : uuid(uuid) {}

    explicit uuid_t(const std::vector<uint8_t>& v)
      : uuid({}) {
        if (v.size() != length) {
            details::throw_out_of_range(
              "Expected size of {} for UUID, got {}", length, v.size());
        }
        std::copy(v.begin(), v.end(), uuid.begin());
    }

    uuid_t() noexcept = default;

    std::vector<uint8_t> to_vector() const { return {uuid.begin(), uuid.end()}; }

    friend void read_nested(
      iobuf_parser& in, uuid_t& u, const size_t bytes_left_limit) {
        using serde::read_nested;
        for (auto& el : u.uuid) {
            uint8_t byte = 0;
            read_nested(in, byte, bytes_left_limit);
            el = byte;
        }
    }

    friend void write(iobuf& out, uuid_t u) {
        using serde::write;
        for (const uint8_t byte : u.uuid) {
          write(out, byte);
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const uuid_t& u) {
        return os << fmt::format("{}", u.uuid);
    }

    friend bool operator==(const uuid_t&, const uuid_t&) = default;

    template<typename H>
    friend H AbslHashValue(H h, const uuid_t& u) {
        for (const uint8_t byte : u.uuid) {
            H tmp = H::combine(std::move(h), byte);
            h = std::move(tmp);
        }
        return h;
    }
};

template<>
struct reflection::adl<uuid_t> {
    void to(iobuf& out, const uuid_t& u) {
        for (const auto b : u.uuid) {
            adl<uint8_t>{}.to(out, b);
        }
    }
    uuid_t from(iobuf_parser& in) {
        uuid_t::underlying_t uuid;
        for (auto& b : uuid) {
            b = adl<uint8_t>{}.from(in);
        }
        return uuid_t(uuid);
    }
};

namespace std {
template<>
struct hash<uuid_t> {
    size_t operator()(const uuid_t& u) {
      return boost::hash<uuid_t::underlying_t>()(u.uuid);
    }
};
} // namespace std
