// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/segment_reader.h"

#include "base/vlog.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/skipping_data_source.h"
#include "storage/record_batch_utils.h"

namespace storage::experimental::mvlog {
segment_reader::segment_reader(
  const version_id inclusive_id, readable_segment* segment)
  : inclusive_id_(inclusive_id)
  , segment_(segment) {
    ++segment_->num_readers_;
}
segment_reader::~segment_reader() { --segment_->num_readers_; }

skipping_data_source::read_list_t
segment_reader::make_read_intervals(size_t start_pos, size_t length) const {
    auto file_gaps = segment_->gaps().gaps_up_to_including(inclusive_id_);
    const size_t max_pos = start_pos + length - 1;
    auto cur_iter_pos = start_pos;
    skipping_data_source::read_list_t read_intervals;

    // Iterate through the gaps associated with the version_id, skipping any
    // that are entirely below the start position.
    while (!file_gaps.empty()) {
        if (cur_iter_pos > max_pos) {
            // The next gap is past the end of the interval.
            break;
        }
        auto gap_it = file_gaps.begin();
        auto next_gap_max = gap_it->end - 1;
        if (cur_iter_pos > next_gap_max) {
            // We are ahead of the next gap. Drop it from the list to consider.
            file_gaps.erase(gap_it);
            continue;
        }
        if (cur_iter_pos < gap_it->start) {
            // The next gap is ahead of us. Read up to the start of it and skip
            // over the gap.
            read_intervals.emplace_back(
              cur_iter_pos, gap_it->start - cur_iter_pos);
            vlog(
              log.info,
              "Reading interval [{}, {})",
              cur_iter_pos,
              gap_it->start);
        }
        // We are in the middle of a gap. Skip to just past the max.
        // Note, end is exclusive.
        cur_iter_pos = gap_it->end;
        file_gaps.erase(gap_it);
        continue;
    }
    read_intervals.emplace_back(cur_iter_pos, length);
    return read_intervals;
}

ss::future<result<std::optional<size_t>, errc>>
segment_reader::find_filepos(model::offset target_offset) {
    auto read_intervals = make_read_intervals(0, segment_->file_->size());
    for (const auto& interval : read_intervals) {
        entry_stream entries(make_stream(interval.offset, interval.length));
        size_t cur_pos = interval.offset;
        while (true) {
            auto entry_res = co_await entries.next();
            if (entry_res.has_error()) {
                co_return entry_res.error();
            }
            auto& entry_opt = entry_res.value();
            if (!entry_opt.has_value()) {
                break;
            }
            const auto body_size = entry_opt->body.size_bytes();
            auto entry_body = serde::from_iobuf<record_batch_entry_body>(
              std::move(entry_opt->body));
            auto batch_header = storage::batch_header_from_disk_iobuf(
              std::move(entry_body.record_batch_header));
            if (batch_header.base_offset >= target_offset) {
                co_return cur_pos;
            }
            cur_pos += body_size + packed_entry_header_size;
        }
    }
    co_return std::nullopt;
}

ss::input_stream<char> segment_reader::make_stream(size_t start_pos) const {
    return make_stream(start_pos, segment_->file_->size() - start_pos);
}

ss::input_stream<char>
segment_reader::make_stream(size_t start_pos, size_t length) const {
    auto read_intervals = make_read_intervals(start_pos, length);
    for (const auto& interval : read_intervals) {
        vlog(
          log.trace,
          "Reading interval [{}, {})",
          interval.offset,
          interval.offset + interval.length);
    }
    return ss::input_stream<char>(
      ss::data_source(std::make_unique<skipping_data_source>(
        segment_->file_, std::move(read_intervals))));
}

} // namespace storage::experimental::mvlog
