// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/log_reader.h"

#include "storage/mvlog/batch_collecting_stream_utils.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/reader_outcome.h"

namespace storage::experimental::mvlog {

ss::future<log_reader::storage_t>
log_reader::do_load_slice(model::timeout_clock::time_point) {
    while (true) {
        if (!cur_entry_stream_) {
            if (seg_readers_.empty()) {
                co_return collector_.release_batches();
            }
            auto& reader = *seg_readers_.begin();
            cur_entry_stream_ = entry_stream{reader->make_stream()};
        }
        auto res = co_await collect_batches_from_stream(
          cur_entry_stream_.value(), collector_);
        if (res.has_error()) {
            throw std::runtime_error("AWONG");
        }
        switch (res.value()) {
        case collect_stream_outcome::end_of_stream:
            // We've exhausted the segment.
            cur_entry_stream_.reset();
            seg_readers_.pop_front();
            continue;
        case collect_stream_outcome::stop:
        case collect_stream_outcome::buffer_full:
            co_return collector_.release_batches();
        }
    }
}

log_reader::~log_reader() = default;

} // namespace storage::experimental::mvlog
