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

#include "serde/parquet/variant_encoding.h"

#include "base/vassert.h"
#include "bytes/iobuf_parser.h"
#include "container/chunked_vector.h"

#include <seastar/core/sstring.hh>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <set>
#include <utility>

namespace serde::parquet {

namespace {

// basic_type values for the value header byte
constexpr uint8_t basic_type_primitive = 0;
constexpr uint8_t basic_type_short_string = 1;
constexpr uint8_t basic_type_object = 2;
constexpr uint8_t basic_type_array = 3;

// primitive type_info values
constexpr uint8_t prim_null = 0;
constexpr uint8_t prim_true = 1;
constexpr uint8_t prim_false = 2;
constexpr uint8_t prim_int32 = 5;
constexpr uint8_t prim_int64 = 6;
constexpr uint8_t prim_double = 7;
constexpr uint8_t prim_float = 14;
constexpr uint8_t prim_binary = 15;
constexpr uint8_t prim_string = 16;

uint8_t make_value_header(uint8_t basic_type, uint8_t type_info) {
    return static_cast<uint8_t>(
      (basic_type & 0x03) | ((type_info & 0x3F) << 2));
}

/// Return the minimum number of bytes (1-4) needed to represent val.
uint8_t min_bytes_for(uint32_t val) {
    if (val <= 0xFF) {
        return 1;
    }
    if (val <= 0xFFFF) {
        return 2;
    }
    if (val <= 0xFFFFFF) {
        return 3;
    }
    return 4;
}

void append_le_uint(iobuf& buf, uint32_t val, uint8_t width) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    uint8_t le[4];
    std::memcpy(le, &val, 4);
    buf.append(le, width);
}

uint32_t read_le_uint(iobuf_const_parser& p, uint8_t width) {
    uint8_t raw[4] = {};
    p.consume_to(width, raw);
    uint32_t val = 0;
    std::memcpy(&val, raw, 4);
    return val;
}

// -- Dictionary builder: collect all object field names --

void collect_field_names(
  const variant_value& val, std::set<ss::sstring>& names) {
    std::visit(
      [&names](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, std::unique_ptr<variant_object>>) {
              for (const auto& [key, field_val] : v->fields) {
                  names.insert(key);
                  collect_field_names(field_val, names);
              }
          } else if constexpr (
            std::is_same_v<T, std::unique_ptr<variant_array>>) {
              for (const auto& elem : v->elements) {
                  collect_field_names(elem, names);
              }
          }
      },
      val);
}

// -- Metadata encoder --

iobuf encode_metadata(const chunked_vector<ss::sstring>& dictionary) {
    // Compute total string data size and max offset for offset_size.
    uint32_t total_string_bytes = 0;
    for (const auto& s : dictionary) {
        total_string_bytes += static_cast<uint32_t>(s.size());
    }

    uint8_t offset_size = min_bytes_for(
      std::max(total_string_bytes, static_cast<uint32_t>(dictionary.size())));

    // Header byte: version=1, sorted_strings=1, offset_size_minus_one
    uint8_t header = 1          // version (bits 0-3)
                     | (1 << 4) // sorted_strings (bit 4)
                     | (static_cast<uint8_t>((offset_size - 1) & 0x03) << 5);

    iobuf meta;
    meta.append(&header, 1);

    // Dictionary size
    append_le_uint(meta, static_cast<uint32_t>(dictionary.size()), offset_size);

    // Offset array: dictionary_size + 1 entries
    uint32_t offset = 0;
    for (size_t i = 0; i <= dictionary.size(); ++i) {
        append_le_uint(meta, offset, offset_size);
        if (i < dictionary.size()) {
            offset += static_cast<uint32_t>(dictionary[i].size());
        }
    }

    // String data
    for (const auto& s : dictionary) {
        meta.append(s.data(), s.size());
    }

    return meta;
}

// -- Value encoder --

void encode_value(
  const variant_value& val,
  const chunked_vector<ss::sstring>& dictionary,
  iobuf& out);

void encode_object(
  const variant_object& obj,
  const chunked_vector<ss::sstring>& dictionary,
  iobuf& out) {
    auto num_fields = static_cast<uint32_t>(obj.fields.size());

    // Find field IDs (indices into dictionary)
    chunked_vector<uint32_t> field_ids;
    field_ids.reserve(num_fields);
    for (const auto& [key, _] : obj.fields) {
        auto it = std::lower_bound(dictionary.begin(), dictionary.end(), key);
        vassert(
          it != dictionary.end() && *it == key,
          "field name not found in dictionary: {}",
          key);
        field_ids.push_back(
          static_cast<uint32_t>(std::distance(dictionary.begin(), it)));
    }

    uint32_t max_field_id = 0;
    for (auto id : field_ids) {
        max_field_id = std::max(max_field_id, id);
    }

    // Encode all field values to a temporary buffer to get offsets.
    chunked_vector<iobuf> field_bufs;
    field_bufs.reserve(num_fields);
    for (const auto& [_, field_val] : obj.fields) {
        iobuf field_buf;
        encode_value(field_val, dictionary, field_buf);
        field_bufs.push_back(std::move(field_buf));
    }

    // Compute field offsets (relative to start of field data).
    chunked_vector<uint32_t> field_offsets;
    field_offsets.reserve(num_fields);
    uint32_t running = 0;
    for (const auto& fb : field_bufs) {
        field_offsets.push_back(running);
        running += static_cast<uint32_t>(fb.size_bytes());
    }

    uint8_t field_offset_size = num_fields > 0 ? min_bytes_for(running)
                                               : uint8_t{1};
    uint8_t field_id_size = num_fields > 0 ? min_bytes_for(max_field_id)
                                           : uint8_t{1};

    // type_info: bits 0-1 = offset_size-1, bits 2-3 = id_size-1
    uint8_t type_info = static_cast<uint8_t>(
      ((field_offset_size - 1) & 0x03) | (((field_id_size - 1) & 0x03) << 2));
    uint8_t header = make_value_header(basic_type_object, type_info);
    out.append(&header, 1);

    // num_fields in field_offset_size bytes
    append_le_uint(out, num_fields, field_offset_size);

    // field_ids array
    for (auto id : field_ids) {
        append_le_uint(out, id, field_id_size);
    }

    // field_offsets array
    for (auto off : field_offsets) {
        append_le_uint(out, off, field_offset_size);
    }

    // field data
    for (auto& fb : field_bufs) {
        out.append(std::move(fb));
    }
}

void encode_array(
  const variant_array& arr,
  const chunked_vector<ss::sstring>& dictionary,
  iobuf& out) {
    auto num_elements = static_cast<uint32_t>(arr.elements.size());

    // Encode each element.
    chunked_vector<iobuf> elem_bufs;
    elem_bufs.reserve(num_elements);
    for (const auto& elem : arr.elements) {
        iobuf elem_buf;
        encode_value(elem, dictionary, elem_buf);
        elem_bufs.push_back(std::move(elem_buf));
    }

    // Compute offsets.
    chunked_vector<uint32_t> offsets;
    offsets.reserve(num_elements);
    uint32_t running = 0;
    for (const auto& eb : elem_bufs) {
        offsets.push_back(running);
        running += static_cast<uint32_t>(eb.size_bytes());
    }

    uint8_t offset_size = num_elements > 0 ? min_bytes_for(running)
                                           : uint8_t{1};
    uint8_t type_info = static_cast<uint8_t>((offset_size - 1) & 0x03);
    uint8_t header = make_value_header(basic_type_array, type_info);
    out.append(&header, 1);

    append_le_uint(out, num_elements, offset_size);

    for (auto off : offsets) {
        append_le_uint(out, off, offset_size);
    }

    for (auto& eb : elem_bufs) {
        out.append(std::move(eb));
    }
}

void encode_value(
  const variant_value& val,
  const chunked_vector<ss::sstring>& dictionary,
  iobuf& out) {
    std::visit(
      [&](const auto& v) {
          using T = std::decay_t<decltype(v)>;
          if constexpr (std::is_same_v<T, null_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_null);
              out.append(&h, 1);
          } else if constexpr (std::is_same_v<T, boolean_value>) {
              uint8_t h = make_value_header(
                basic_type_primitive, v.val ? prim_true : prim_false);
              out.append(&h, 1);
          } else if constexpr (std::is_same_v<T, int32_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_int32);
              out.append(&h, 1);
              int32_t le = v.val;
              // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
              out.append(reinterpret_cast<const char*>(&le), sizeof(le));
          } else if constexpr (std::is_same_v<T, int64_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_int64);
              out.append(&h, 1);
              int64_t le = v.val;
              // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
              out.append(reinterpret_cast<const char*>(&le), sizeof(le));
          } else if constexpr (std::is_same_v<T, float32_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_float);
              out.append(&h, 1);
              float le = v.val;
              // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
              out.append(reinterpret_cast<const char*>(&le), sizeof(le));
          } else if constexpr (std::is_same_v<T, float64_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_double);
              out.append(&h, 1);
              double le = v.val;
              // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
              out.append(reinterpret_cast<const char*>(&le), sizeof(le));
          } else if constexpr (std::is_same_v<T, variant_string_value>) {
              if (v.val.size() < 64) {
                  uint8_t h = make_value_header(
                    basic_type_short_string,
                    static_cast<uint8_t>(v.val.size()));
                  out.append(&h, 1);
                  out.append(v.val.data(), v.val.size());
              } else {
                  uint8_t h = make_value_header(
                    basic_type_primitive, prim_string);
                  out.append(&h, 1);
                  auto len = static_cast<uint32_t>(v.val.size());
                  append_le_uint(out, len, 4);
                  out.append(v.val.data(), v.val.size());
              }
          } else if constexpr (std::is_same_v<T, byte_array_value>) {
              uint8_t h = make_value_header(basic_type_primitive, prim_binary);
              out.append(&h, 1);
              auto len = static_cast<uint32_t>(v.val.size_bytes());
              append_le_uint(out, len, 4);
              // Copy iobuf contents
              for (const auto& frag : v.val) {
                  out.append(frag.get(), frag.size());
              }
          } else if constexpr (
            std::is_same_v<T, std::unique_ptr<variant_object>>) {
              encode_object(*v, dictionary, out);
          } else if constexpr (
            std::is_same_v<T, std::unique_ptr<variant_array>>) {
              encode_array(*v, dictionary, out);
          }
      },
      val);
}

// -- Decoder --

chunked_vector<ss::sstring> decode_dictionary(iobuf_const_parser& p) {
    uint8_t header = p.consume_type<uint8_t>();
    uint8_t version = header & 0x0F;
    vassert(version == 1, "unsupported variant metadata version: {}", version);

    uint8_t offset_size = static_cast<uint8_t>(((header >> 5) & 0x03) + 1);

    uint32_t dict_size = read_le_uint(p, offset_size);

    // Read offset array: dict_size + 1 entries
    chunked_vector<uint32_t> offsets;
    offsets.reserve(dict_size + 1);
    for (uint32_t i = 0; i <= dict_size; ++i) {
        offsets.push_back(read_le_uint(p, offset_size));
    }

    // Read string data using offsets
    chunked_vector<ss::sstring> dictionary;
    dictionary.reserve(dict_size);
    for (uint32_t i = 0; i < dict_size; ++i) {
        uint32_t len = offsets[i + 1] - offsets[i];
        dictionary.push_back(p.read_string_unsafe(len));
    }

    return dictionary;
}

variant_value decode_value(
  iobuf_const_parser& p, const chunked_vector<ss::sstring>& dictionary);

variant_value decode_object(
  iobuf_const_parser& p,
  uint8_t type_info,
  const chunked_vector<ss::sstring>& dictionary) {
    uint8_t field_offset_size = static_cast<uint8_t>((type_info & 0x03) + 1);
    uint8_t field_id_size = static_cast<uint8_t>(((type_info >> 2) & 0x03) + 1);

    uint32_t num_fields = read_le_uint(p, field_offset_size);

    chunked_vector<uint32_t> field_ids;
    field_ids.reserve(num_fields);
    for (uint32_t i = 0; i < num_fields; ++i) {
        field_ids.push_back(read_le_uint(p, field_id_size));
    }

    chunked_vector<uint32_t> field_offsets;
    field_offsets.reserve(num_fields);
    for (uint32_t i = 0; i < num_fields; ++i) {
        field_offsets.push_back(read_le_uint(p, field_offset_size));
    }

    auto obj = std::make_unique<variant_object>();
    obj->fields.reserve(num_fields);
    for (uint32_t i = 0; i < num_fields; ++i) {
        vassert(
          field_ids[i] < dictionary.size(),
          "field id {} out of range (dictionary size {})",
          field_ids[i],
          dictionary.size());
        const auto& key = dictionary[field_ids[i]];
        auto field_val = decode_value(p, dictionary);
        obj->fields.emplace_back(key, std::move(field_val));
    }

    return obj;
}

variant_value decode_array(
  iobuf_const_parser& p,
  uint8_t type_info,
  const chunked_vector<ss::sstring>& dictionary) {
    uint8_t offset_size = static_cast<uint8_t>((type_info & 0x03) + 1);

    uint32_t num_elements = read_le_uint(p, offset_size);

    // Read offsets (we don't strictly need them for sequential decode,
    // but we consume them to advance the parser).
    for (uint32_t i = 0; i < num_elements; ++i) {
        read_le_uint(p, offset_size);
    }

    auto arr = std::make_unique<variant_array>();
    arr->elements.reserve(num_elements);
    for (uint32_t i = 0; i < num_elements; ++i) {
        arr->elements.push_back(decode_value(p, dictionary));
    }

    return arr;
}

variant_value decode_value(
  iobuf_const_parser& p, const chunked_vector<ss::sstring>& dictionary) {
    uint8_t header = p.consume_type<uint8_t>();
    uint8_t basic_type = header & 0x03;
    uint8_t type_info = (header >> 2) & 0x3F;

    switch (basic_type) {
    case basic_type_primitive:
        switch (type_info) {
        case prim_null:
            return null_value{};
        case prim_true:
            return boolean_value{.val = true};
        case prim_false:
            return boolean_value{.val = false};
        case prim_int32: {
            auto val = p.consume_type<int32_t>();
            return int32_value{.val = val};
        }
        case prim_int64: {
            auto val = p.consume_type<int64_t>();
            return int64_value{.val = val};
        }
        case prim_double: {
            auto val = p.consume_type<double>();
            return float64_value{.val = val};
        }
        case prim_float: {
            auto val = p.consume_type<float>();
            return float32_value{.val = val};
        }
        case prim_binary: {
            uint32_t len = read_le_uint(p, 4);
            iobuf data = p.copy(len);
            return byte_array_value{.val = std::move(data)};
        }
        case prim_string: {
            uint32_t len = read_le_uint(p, 4);
            auto str = p.read_string_unsafe(len);
            return variant_string_value{.val = std::move(str)};
        }
        default:
            vassert(false, "unsupported primitive type_info: {}", type_info);
        }
    case basic_type_short_string: {
        auto str = p.read_string_unsafe(type_info);
        return variant_string_value{.val = std::move(str)};
    }
    case basic_type_object:
        return decode_object(p, type_info, dictionary);
    case basic_type_array:
        return decode_array(p, type_info, dictionary);
    default:
        vassert(false, "unsupported basic_type: {}", basic_type);
    }
}

} // namespace

encoded_variant encode_variant(const variant_value& val) {
    // Collect and sort all field names for the dictionary.
    std::set<ss::sstring> name_set;
    collect_field_names(val, name_set);

    chunked_vector<ss::sstring> dictionary;
    dictionary.reserve(name_set.size());
    for (auto& s : name_set) {
        dictionary.push_back(s);
    }

    iobuf metadata = encode_metadata(dictionary);

    iobuf value_buf;
    encode_value(val, dictionary, value_buf);

    return encoded_variant{
      .metadata = std::move(metadata),
      .value = std::move(value_buf),
    };
}

variant_value decode_variant(const iobuf& metadata, const iobuf& value) {
    iobuf_const_parser meta_parser(metadata);
    auto dictionary = decode_dictionary(meta_parser);

    iobuf_const_parser val_parser(value);
    return decode_value(val_parser, dictionary);
}

} // namespace serde::parquet
