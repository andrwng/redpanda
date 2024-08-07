// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "iceberg/values_avro.h"

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "iceberg/values.h"

#include <avro/GenericDatum.hh>
#include <avro/Schema.hh>

namespace iceberg {

namespace {

// XXX: fixed types in Avro need to construct a NodePtr to construct an
// avro::GenericFixed.
struct primitive_value_avro_visitor {
    template<typename ValueT>
    avro::GenericDatum operator()(const ValueT& v) {
        return {v.val};
    }
    avro::GenericDatum operator()(const string_value& v) {
        const auto size_bytes = v.val.size_bytes();
        iobuf_const_parser buf(v.val);
        return {buf.read_string(size_bytes)};
    }
    avro::GenericDatum operator()(const binary_value& v) {
        std::vector<uint8_t> data;
        auto bytes = iobuf_to_bytes(v.val);
        data.resize(bytes.size());
        for (size_t i = 0; i < bytes.size(); i++) {
            data[i] = bytes[i];
        }
        return {data};
    }
    avro::GenericDatum operator()(const decimal_value&) {
        throw std::invalid_argument("XXX decimal not supported");
    }
    avro::GenericDatum operator()(const fixed_value&) {
        throw std::invalid_argument("XXX fixed not supported");
    }
    avro::GenericDatum operator()(const date_value& v) { return {v.val}; }
    avro::GenericDatum operator()(const time_value& v) { return {v.val}; }
    avro::GenericDatum operator()(const timestamp_value& v) { return {v.val}; }
    avro::GenericDatum operator()(const timestamptz_value& v) {
        return {v.val};
    }
    avro::GenericDatum operator()(const uuid_value&) {
        throw std::invalid_argument("XXX uuid not supported");
    }
};

struct value_avro_visitor {
    explicit value_avro_visitor(const avro::NodePtr& avro_schema)
      : avro_schema_(avro_schema) {}

    // Expected to match with the value on which the caller is calling
    // operator().
    const avro::NodePtr& avro_schema_;

    avro::GenericDatum operator()(const primitive_value& v) {
        return std::visit(primitive_value_avro_visitor{}, v);
    }
    avro::GenericDatum operator()(const struct_value& v) {
        avro::GenericDatum datum(avro_schema_);
        auto& record = datum.value<avro::GenericRecord>();
        if (record.fieldCount() != v.fields.size()) {
            throw std::invalid_argument(fmt::format(
              "Struct value does not match given Avro schema: {} "
              "fields vs expected {}",
              v.fields.size(),
              record.fieldCount()));
        }
        for (size_t i = 0; i < v.fields.size(); i++) {
            const auto& child_val_ptr = v.fields[i];
            const auto& child_schema = avro_schema_->leafAt(i);
            record.setFieldAt(i, value_as_avro(child_val_ptr, child_schema));
        }
        return datum;
    }
    avro::GenericDatum operator()(const list_value& v) {
        avro::GenericDatum datum(avro_schema_);
        auto& arr = datum.value<avro::GenericArray>();
        const auto& child_schema = avro_schema_->leafAt(0);
        for (const auto& element_ptr : v.elements) {
            arr.value().emplace_back(value_as_avro(element_ptr, child_schema));
        }
        return datum;
    }
    avro::GenericDatum operator()(const map_value& v) {
        avro::GenericDatum datum(avro_schema_);
        auto& m = datum.value<avro::GenericArray>();
        const auto& kv_schema = avro_schema_->leafAt(0);
        for (const auto& kv_ptr : v.kvs) {
            avro::GenericDatum kv_datum(kv_schema);
            auto& kv_record = kv_datum.value<avro::GenericRecord>();
            kv_record.setFieldAt(
              0, value_as_avro(kv_ptr.key, kv_schema->leafAt(0)));
            kv_record.setFieldAt(
              1, value_as_avro(kv_ptr.val, kv_schema->leafAt(1)));
            m.value().emplace_back(std::move(kv_datum));
        }
        return datum;
    }
};

} // namespace

avro::GenericDatum
value_as_avro(const value_ptr& val, const avro::NodePtr& avro_schema) {
    if (avro_schema->type() == avro::AVRO_UNION) {
        avro::GenericDatum ret(avro_schema);
        auto& un = ret.value<avro::GenericUnion>();
        if (val == nullptr) {
            un.selectBranch(0);
            return ret;
        }
        un.selectBranch(1);
        un.datum() = value_as_avro(val, avro_schema->leafAt(1));
        return ret;
    }
    if (val == nullptr) {
        throw std::invalid_argument(
          "Value is null but expected non-union type");
    }
    return std::visit(value_avro_visitor{avro_schema}, *val);
}

} // namespace iceberg
