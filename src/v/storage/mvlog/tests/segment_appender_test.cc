// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "container/fragmented_vector.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "storage/mvlog/entry_stream_utils.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/segment_appender.h"
#include "storage/record_batch_utils.h"
#include "test_utils/gtest_utils.h"

#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace ::experimental;

namespace {
truncation_entry_body copy(const truncation_entry_body& b) {
    truncation_entry_body ret;
    ret.base_offset = b.base_offset;
    ret.gaps = b.gaps.copy();
    return ret;
}
} // anonymous namespace

class SegmentAppenderTest : public ::testing::Test {
public:
    SegmentAppenderTest()
      : base_dir_(get_test_directory())
      , file_({fmt::format("{}/segment", base_dir_)}) {}

    void SetUp() override {
        cleanup_files_.emplace_back(file_);
        ss::touch_directory(base_dir_).get();
        paging_file_ = file_manager_.create_file(file_).get();
    }
    void TearDown() override {
        paging_file_->close().get();
        ss::recursive_remove_directory({base_dir_}).get();
    }

    truncation_entry_body make_random_truncation_body() {
        truncation_entry_body ret;
        ret.base_offset = model::offset{random_generators::get_int(1024)};
        auto num_gaps = random_generators::get_int(1024);
        for (int i = 0; i < num_gaps; i++) {
            segment_truncation seg_trunc;
            seg_trunc.segment_id = segment_id{random_generators::get_int(1024)};
            seg_trunc.gap = file_gap(
              random_generators::get_int(1024),
              random_generators::get_int(1024));
            ret.gaps.emplace_back(seg_trunc);
        }
        return ret;
    }

protected:
    const ss::sstring base_dir_;
    const std::filesystem::path file_;
    file_manager file_manager_;
    std::unique_ptr<file> paging_file_;
    std::vector<std::filesystem::path> cleanup_files_;
};

TEST_F(SegmentAppenderTest, TestAppendRecordBatches) {
    segment_appender appender(paging_file_.get());
    auto batches = model::test::make_random_batches().get();

    size_t prev_end_pos = 0;
    for (const auto& batch : batches) {
        // Construct the record body so we can compare against it.
        record_batch_entry_body entry_body;
        entry_body.term = batch.term();
        entry_body.record_batch_header.append(
          storage::batch_header_to_disk_iobuf(batch.header()));
        entry_body.records.append(batch.copy().release_data());
        auto entry_body_buf = serde::to_iobuf(std::move(entry_body));

        appender.append(batch.copy()).get();
        ASSERT_EQ(
          paging_file_->size(),
          prev_end_pos + entry_body_buf.size_bytes()
            + packed_entry_header_size);

        // The resulting pages should contain the header...
        auto hdr_stream = paging_file_->make_stream(
          prev_end_pos, packed_entry_header_size);
        iobuf hdr_buf;
        hdr_buf.append(hdr_stream.read_exactly(packed_entry_header_size).get());

        auto hdr = entry_header_from_iobuf(std::move(hdr_buf));
        ASSERT_EQ(hdr.body_size, entry_body_buf.size_bytes());
        ASSERT_EQ(hdr.type, entry_type::record_batch);
        ASSERT_NE(hdr.header_crc, 0);
        ASSERT_EQ(entry_header_crc(hdr.body_size, hdr.type), hdr.header_crc);

        // ... followed by the entry body.
        auto body_stream = paging_file_->make_stream(
          prev_end_pos + packed_entry_header_size, entry_body_buf.size_bytes());
        iobuf body_buf;
        body_buf.append(
          body_stream.read_exactly(entry_body_buf.size_bytes()).get());
        ASSERT_EQ(entry_body_buf, body_buf);
        prev_end_pos = paging_file_->size();
    }
}

TEST_F(SegmentAppenderTest, TestTruncate) {
    segment_appender appender(paging_file_.get());
    constexpr int num_truncations = 10;
    chunked_vector<truncation_entry_body> truncations;
    truncations.reserve(num_truncations);
    for (int i = 0; i < num_truncations; i++) {
        truncations.emplace_back(make_random_truncation_body());
    }

    size_t prev_end_pos = 0;
    for (const auto& t : truncations) {
        auto entry_body_buf = serde::to_iobuf(copy(t));
        appender.truncate(copy(t)).get();
        ASSERT_EQ(
          paging_file_->size(),
          prev_end_pos + entry_body_buf.size_bytes()
            + packed_entry_header_size);

        // The resulting pages should contain the header...
        auto hdr_stream = paging_file_->make_stream(
          prev_end_pos, packed_entry_header_size);
        iobuf hdr_buf;
        hdr_buf.append(hdr_stream.read_exactly(packed_entry_header_size).get());

        auto hdr = entry_header_from_iobuf(std::move(hdr_buf));
        ASSERT_EQ(hdr.body_size, entry_body_buf.size_bytes());
        ASSERT_EQ(hdr.type, entry_type::truncation);
        ASSERT_NE(hdr.header_crc, 0);
        ASSERT_EQ(entry_header_crc(hdr.body_size, hdr.type), hdr.header_crc);

        // ... followed by the entry body.
        auto body_stream = paging_file_->make_stream(
          prev_end_pos + packed_entry_header_size, entry_body_buf.size_bytes());
        iobuf body_buf;
        body_buf.append(
          body_stream.read_exactly(entry_body_buf.size_bytes()).get());
        ASSERT_EQ(entry_body_buf, body_buf);
        prev_end_pos = paging_file_->size();
    }
}
