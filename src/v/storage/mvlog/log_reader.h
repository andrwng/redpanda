// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "model/record_batch_reader.h"
#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/entry_stream.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/reader_outcome.h"
#include "storage/mvlog/segment_reader.h"
#include "storage/mvlog/version_id.h"

#include <seastar/core/shared_ptr.hh>

namespace storage::experimental::mvlog {

class readable_segment;

class log_reader final : public model::record_batch_reader::impl {
public:
    using data_t = model::record_batch_reader::data_t;
    using foreign_data_t = model::record_batch_reader::foreign_data_t;
    using storage_t = model::record_batch_reader::storage_t;

    explicit log_reader(
      const log_reader_config& cfg,
      version_id tid,
      ss::chunked_fifo<std::unique_ptr<segment_reader>> seg_readers)
      : cfg_(cfg)
      , tid_(tid)
      , collector_(cfg_, model::term_id{})
      , seg_readers_(std::move(seg_readers)) {}
    ~log_reader() final;
    bool is_end_of_stream() const final { return seg_readers_.empty(); }
    ss::future<storage_t> do_load_slice(model::timeout_clock::time_point) final;
    ss::future<> finally() noexcept final { return ss::make_ready_future<>(); }
    void print(std::ostream& os) final { fmt::print(os, "mvlog::log_reader"); }

private:
    const log_reader_config cfg_;
    const version_id tid_;
    batch_collector collector_;
    ss::chunked_fifo<std::unique_ptr<segment_reader>> seg_readers_;
    std::optional<entry_stream> cur_entry_stream_;
};

} // namespace storage::experimental::mvlog
