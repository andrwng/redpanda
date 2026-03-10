/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/logger.h"
#include "lsm/core/exceptions.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

#include <exception>

namespace cloud_topics::l1 {

using errc = state_reader::errc;
using error = state_reader::error;

namespace {

error to_error(std::exception_ptr e, std::string_view prefix = "") {
    if (ssx::is_shutdown_exception(e)) {
        return {errc::shutting_down, "{}{}", prefix, e};
    }
    errc ec{};
    ss::sstring msg;
    try {
        std::rethrow_exception(e);
    } catch (lsm::abort_requested_exception& ex) {
        ec = errc::shutting_down;
        msg = fmt::format("{}{}", prefix, ex.what());
    } catch (lsm::corruption_exception& ex) {
        ec = errc::corruption;
        msg = fmt::format("{}{}", prefix, ex.what());
    } catch (std::exception& ex) {
        ec = errc::io_error;
        msg = fmt::format("{}{}", prefix, ex.what());
        vlog(cd_log.error, "Unexpected exception: {}", msg);
    } catch (...) {
        ec = errc::io_error;
        msg = fmt::format("{}{}", prefix, e);
        vlog(cd_log.error, "Unexpected exception_ptr: {}", msg);
    }
    return {ec, std::move(msg)};
}

} // namespace

ss::future<
  std::expected<chunked_vector<model::topic_id_partition>, debug_reader::error>>
debug_reader::get_all_partitions() {
    chunked_vector<model::topic_id_partition> result;
    try {
        auto iter = co_await reader_.snap_.create_iterator();
        co_await iter.seek(
          metadata_row_key::encode(
            model::topic_id_partition(
              model::topic_id(uuid_t{}), model::partition_id(0))));

        while (iter.valid()) {
            auto key = metadata_row_key::decode(iter.key());
            if (!key.has_value()) {
                break;
            }
            result.push_back(key->tidp);
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }
    co_return result;
}

ss::future<std::expected<debug_reader::partition_summary, debug_reader::error>>
debug_reader::get_partition_summary(const model::topic_id_partition& tp) {
    partition_summary summary;
    summary.tp = tp;

    auto meta_res = co_await reader_.get_metadata(tp);
    if (!meta_res) {
        co_return std::unexpected(std::move(meta_res.error()));
    }
    if (!meta_res.value()) {
        co_return std::unexpected(
          error(state_reader::errc::corruption, "No metadata for {}", tp));
    }
    summary.metadata = *meta_res.value();

    // Iterate extents to accumulate counts/bounds/sizes.
    auto extents_res = co_await reader_.get_inclusive_extents(
      tp, std::nullopt, std::nullopt);
    if (!extents_res) {
        co_return std::unexpected(std::move(extents_res.error()));
    }
    if (extents_res.value()) {
        auto rows = co_await extents_res.value()->materialize_rows();
        for (auto& row_res : rows) {
            if (!row_res) {
                co_return std::unexpected(std::move(row_res.error()));
            }
            auto& row = *row_res;
            auto decoded_key = extent_row_key::decode(row.key);
            if (!decoded_key) {
                co_return std::unexpected(error(
                  state_reader::errc::corruption,
                  "Failed to decode extent key {}",
                  row.key));
            }
            kafka::offset base = decoded_key->base_offset;
            kafka::offset last = row.val.last_offset;

            if (summary.extent_count == 0) {
                summary.extent_min_offset = base;
                summary.extent_max_offset = last;
            } else {
                if (base < summary.extent_min_offset) {
                    summary.extent_min_offset = base;
                }
                if (last > summary.extent_max_offset) {
                    summary.extent_max_offset = last;
                }
            }
            summary.total_extent_data_size += row.val.len;
            ++summary.extent_count;
        }
    }

    // Iterate terms via raw key space for term_id and start_offset.
    try {
        auto iter = co_await reader_.snap_.create_iterator();
        co_await iter.seek(term_row_key::encode(tp, model::term_id(0)));
        while (iter.valid()) {
            auto key = term_row_key::decode(iter.key());
            if (!key.has_value() || key->tidp != tp) {
                break;
            }
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            if (summary.term_count == 0) {
                summary.min_term = key->term;
                summary.min_term_start_offset = val.term_start_offset;
            }
            summary.max_term = key->term;
            summary.max_term_start_offset = val.term_start_offset;
            ++summary.term_count;
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }

    // Read compaction state.
    auto comp_res = co_await reader_.get_compaction_metadata(tp);
    if (!comp_res) {
        co_return std::unexpected(std::move(comp_res.error()));
    }
    if (comp_res.value()) {
        summary.has_compaction_state = true;
        auto& cs = *comp_res.value();
        summary.cleaned_range_count = cs.cleaned_ranges.to_vec().size();
        summary.tombstone_range_count
          = cs.cleaned_ranges_with_tombstones.size();
    }

    co_return summary;
}

ss::future<std::expected<debug_reader::partition_dump, debug_reader::error>>
debug_reader::dump_partition(const model::topic_id_partition& tp) {
    partition_dump dump;
    dump.tp = tp;

    auto meta_res = co_await reader_.get_metadata(tp);
    if (!meta_res) {
        co_return std::unexpected(std::move(meta_res.error()));
    }
    if (!meta_res.value()) {
        co_return std::unexpected(
          error(state_reader::errc::corruption, "No metadata for {}", tp));
    }
    dump.metadata = *meta_res.value();

    // Materialize all extents.
    auto extents_res = co_await reader_.get_inclusive_extents(
      tp, std::nullopt, std::nullopt);
    if (!extents_res) {
        co_return std::unexpected(std::move(extents_res.error()));
    }
    if (extents_res.value()) {
        auto rows = co_await extents_res.value()->materialize_rows();
        for (auto& row_res : rows) {
            if (!row_res) {
                co_return std::unexpected(std::move(row_res.error()));
            }
            auto& row = *row_res;
            auto decoded_key = extent_row_key::decode(row.key);
            if (!decoded_key) {
                co_return std::unexpected(error(
                  state_reader::errc::corruption,
                  "Failed to decode extent key {}",
                  row.key));
            }
            dump.extents.push_back(
              extent{
                .base_offset = decoded_key->base_offset,
                .last_offset = row.val.last_offset,
                .max_timestamp = row.val.max_timestamp,
                .filepos = row.val.filepos,
                .len = row.val.len,
                .oid = row.val.oid,
              });
        }
    }

    // Iterate term key space for key+value access.
    try {
        auto iter = co_await reader_.snap_.create_iterator();
        co_await iter.seek(term_row_key::encode(tp, model::term_id(0)));
        while (iter.valid()) {
            auto key = term_row_key::decode(iter.key());
            if (!key.has_value() || key->tidp != tp) {
                break;
            }
            auto val = serde::from_iobuf<term_row_value>(iter.value());
            dump.term_starts.push_back(
              term_start{
                .term_id = key->term,
                .start_offset = val.term_start_offset,
              });
            co_await iter.next();
        }
    } catch (...) {
        co_return std::unexpected(to_error(std::current_exception()));
    }

    auto comp_res = co_await reader_.get_compaction_metadata(tp);
    if (!comp_res) {
        co_return std::unexpected(std::move(comp_res.error()));
    }
    dump.compaction = std::move(comp_res.value());

    co_return dump;
}

ss::future<std::expected<
  chunked_vector<std::pair<object_id, object_entry>>,
  debug_reader::error>>
debug_reader::get_objects(const chunked_vector<object_id>& oids) {
    chunked_vector<std::pair<object_id, object_entry>> result;
    for (const auto& oid : oids) {
        auto obj_res = co_await reader_.get_object(oid);
        if (!obj_res) {
            co_return std::unexpected(std::move(obj_res.error()));
        }
        if (obj_res.value()) {
            result.emplace_back(oid, std::move(*obj_res.value()));
        }
    }
    co_return result;
}

chunked_vector<debug_reader::invariant_violation>
debug_reader::check_invariants(const partition_dump& dump) {
    chunked_vector<invariant_violation> violations;

    if (dump.extents.empty()) {
        return violations;
    }

    const auto& first_extent = dump.extents.front();
    const auto& last_extent = dump.extents.back();

    for (size_t i = 1; i < dump.extents.size(); ++i) {
        const auto& prev = dump.extents[i - 1];
        const auto& curr = dump.extents[i];

        if (curr.base_offset <= prev.last_offset) {
            violations.push_back(
              invariant_violation{
                .check_name = "extent_overlap",
                .description = fmt::format(
                  "Extents overlap: [{}, {}] and [{}, {}]",
                  prev.base_offset,
                  prev.last_offset,
                  curr.base_offset,
                  curr.last_offset),
              });
        } else if (curr.base_offset != kafka::next_offset(prev.last_offset)) {
            violations.push_back(
              invariant_violation{
                .check_name = "extent_gap",
                .description = fmt::format(
                  "Gap between extents: [{}, {}] and [{}, {}]",
                  prev.base_offset,
                  prev.last_offset,
                  curr.base_offset,
                  curr.last_offset),
              });
        }
    }

    if (
      dump.metadata.next_offset
      != kafka::next_offset(last_extent.last_offset)) {
        violations.push_back(
          invariant_violation{
            .check_name = "next_offset_mismatch",
            .description = fmt::format(
              "next_offset {} != last_extent.last_offset + 1 ({})",
              dump.metadata.next_offset,
              kafka::next_offset(last_extent.last_offset)),
          });
    }

    if (dump.metadata.start_offset < first_extent.base_offset) {
        violations.push_back(
          invariant_violation{
            .check_name = "start_offset_before_extents",
            .description = fmt::format(
              "start_offset {} < first extent base_offset {}",
              dump.metadata.start_offset,
              first_extent.base_offset),
          });
    }

    for (size_t i = 1; i < dump.term_starts.size(); ++i) {
        const auto& prev = dump.term_starts[i - 1];
        const auto& curr = dump.term_starts[i];

        if (curr.term_id <= prev.term_id) {
            violations.push_back(
              invariant_violation{
                .check_name = "term_id_not_increasing",
                .description = fmt::format(
                  "Term IDs not strictly increasing: {} then {}",
                  prev.term_id,
                  curr.term_id),
              });
        }
        if (curr.start_offset <= prev.start_offset) {
            violations.push_back(
              invariant_violation{
                .check_name = "term_offset_not_increasing",
                .description = fmt::format(
                  "Term start offsets not strictly increasing: term {} at {} "
                  "then term {} at {}",
                  prev.term_id,
                  prev.start_offset,
                  curr.term_id,
                  curr.start_offset),
              });
        }
    }

    // Check term starts align with extent boundaries.
    if (!dump.term_starts.empty() && !dump.extents.empty()) {
        chunked_hash_map<kafka::offset, bool> extent_bases;
        for (const auto& ext : dump.extents) {
            extent_bases[ext.base_offset] = true;
        }
        for (size_t i = 1; i < dump.term_starts.size(); ++i) {
            auto so = dump.term_starts[i].start_offset;
            if (so >= first_extent.base_offset && !extent_bases.contains(so)) {
                violations.push_back(
                  invariant_violation{
                    .check_name = "term_not_at_extent_boundary",
                    .description = fmt::format(
                      "Term {} start offset {} does not align with any extent "
                      "base_offset",
                      dump.term_starts[i].term_id,
                      so),
                  });
            }
        }
    }

    if (dump.compaction) {
        auto cleaned_vec = dump.compaction->cleaned_ranges.to_vec();
        for (const auto& range : cleaned_vec) {
            if (
              range.base_offset < first_extent.base_offset
              || range.last_offset > last_extent.last_offset) {
                violations.push_back(
                  invariant_violation{
                    .check_name = "compaction_out_of_range",
                    .description = fmt::format(
                      "Cleaned range [{}, {}] outside extent bounds [{}, {}]",
                      range.base_offset,
                      range.last_offset,
                      first_extent.base_offset,
                      last_extent.last_offset),
                  });
            }
        }
    }

    return violations;
}

chunked_vector<debug_reader::invariant_violation>
debug_reader::check_object_references(
  const partition_dump& dump,
  const chunked_hash_map<object_id, object_entry>& known_objects) {
    chunked_vector<invariant_violation> violations;

    for (const auto& ext : dump.extents) {
        if (!known_objects.contains(ext.oid)) {
            violations.push_back(
              invariant_violation{
                .check_name = "missing_object_entry",
                .description = fmt::format(
                  "Extent [{}, {}] references object {} which has no entry",
                  ext.base_offset,
                  ext.last_offset,
                  ext.oid),
              });
        }
    }

    return violations;
}

} // namespace cloud_topics::l1
