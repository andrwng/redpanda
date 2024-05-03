// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/segment_reader.h"
#include "storage/mvlog/version_id.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;
using namespace experimental;

class SegmentReaderTest : public ::testing::Test {
public:
    void SetUp() override {
        cleanup_files_.emplace_back(file_);
        paging_file_ = file_manager_.create_file(file_).get();
    }

    void TearDown() override {
        paging_file_->close().get();

        for (auto& file : cleanup_files_) {
            try {
                ss::remove_file(file.string()).get();
            } catch (...) {
            }
        }
    }

protected:
    const std::filesystem::path file_{"segment"};
    file_manager file_manager_;
    std::unique_ptr<file> paging_file_;
    std::vector<std::filesystem::path> cleanup_files_;
};

TEST_F(SegmentReaderTest, TestCountReaders) {
    readable_segment readable_seg(paging_file_.get());
    ASSERT_EQ(0, readable_seg.num_readers());
    {
        auto reader = readable_seg.make_reader(version_id{0});
        ASSERT_EQ(1, readable_seg.num_readers());
    }
    ASSERT_EQ(0, readable_seg.num_readers());
    std::vector<std::unique_ptr<segment_reader>> readers;
    readers.reserve(10);
    for (int i = 0; i < 10; i++) {
        auto reader = readable_seg.make_reader(version_id{0});
        ASSERT_EQ(i + 1, readable_seg.num_readers());
        readers.emplace_back(std::move(reader));
    }
    ASSERT_EQ(10, readable_seg.num_readers());
    readers.clear();
    ASSERT_EQ(0, readable_seg.num_readers());
}

TEST_F(SegmentReaderTest, TestEmptyRead) {
    readable_segment readable_seg(paging_file_.get());
    auto reader = readable_seg.make_reader(version_id{0});
    auto stream = reader->make_stream();
    auto buf = stream.read().get();
    ASSERT_TRUE(buf.empty());
}

TEST_F(SegmentReaderTest, TestBasicReads) {
    ss::sstring data = "0123456789";
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());
    for (int i = 0; i < data.size(); i++) {
        auto reader = readable_seg.make_reader(version_id{0});
        auto stream = reader->make_stream(i);
        auto buf = stream.read_up_to(data.size()).get();

        auto expected_str = data.substr(i);
        ss::temporary_buffer<char> expected_buf{
          expected_str.begin(), expected_str.size()};
        ASSERT_EQ(buf, expected_buf);
    }
}

namespace {
// Checks that a given reader starting at the given position reads exactly the
// expected string.
void validate_reader_exactly(
  segment_reader& reader, size_t start_pos, const ss::sstring& expected_str) {
    // The stream should match exactly. Try to read more the expected length to
    // ensure the stream stops where it should.
    auto stream = reader.make_stream(start_pos);
    auto buf = stream.read_exactly(expected_str.size() + 1).get();
    ss::temporary_buffer<char> expected_buf{
      expected_str.begin(), expected_str.size()};
    ASSERT_EQ(buf, expected_buf) << fmt::format(
      "{} vs {}", ss::sstring(buf.get(), buf.size()), expected_str);
}

using expectations_map_t = absl::btree_map<version_id, ss::sstring>;
// Checks that a read as of a specific version matches what is expected of that
// version.
void validate_all_versions_exactly(
  readable_segment& readable_seg,
  const expectations_map_t& expected_per_version) {
    for (const auto& [vid, expected_str] : expected_per_version) {
        auto reader = readable_seg.make_reader(vid);
        ASSERT_NO_FATAL_FAILURE(
          validate_reader_exactly(*reader, 0, expected_str));
    }
}
} // anonymous namespace

// Test reading across gaps.
TEST_F(SegmentReaderTest, TestBasicReadsWithGaps) {
    const ss::sstring data = "0123456789";
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());

    expectations_map_t expected_per_version;
    readable_seg.mutable_gaps()->add(file_gap(1, 3), version_id{1});
    expected_per_version[version_id{1}] = "0456789";
    ASSERT_NO_FATAL_FAILURE(
      validate_all_versions_exactly(readable_seg, expected_per_version));

    readable_seg.mutable_gaps()->add(file_gap(6, 2), version_id{2});
    expected_per_version[version_id{2}] = "04589";
    ASSERT_NO_FATAL_FAILURE(
      validate_all_versions_exactly(readable_seg, expected_per_version));

    readable_seg.mutable_gaps()->add(file_gap(0, 10), version_id{3});
    expected_per_version[version_id{3}] = "";
    ASSERT_NO_FATAL_FAILURE(
      validate_all_versions_exactly(readable_seg, expected_per_version));
}

TEST_F(SegmentReaderTest, TestReadVersionBeforeGaps) {
    const ss::sstring data = "0123456789";
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());

    expectations_map_t expected_per_version;
    readable_seg.mutable_gaps()->add(file_gap(1, 3), version_id{1});
    for (int i = 0; i < data.size(); i++) {
        // Reading below the gapped version should ignore the gap entirely.
        auto reader = readable_seg.make_reader(version_id{0});
        ASSERT_NO_FATAL_FAILURE(
          validate_reader_exactly(*reader, i, data.substr(i)));
    }
}

// Test reading in and around a gap.
TEST_F(SegmentReaderTest, TestReadInsideGap) {
    const ss::sstring data = "0123456789";
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());

    // Remove [2, 4].
    readable_seg.mutable_gaps()->add(file_gap(2, 3), version_id{1});
    for (int i = 2; i <= 4; i++) {
        auto reader = readable_seg.make_reader(version_id{1});
        // If the start of the read is inside a gap, it should skip to the end
        // of the gap. Attempt reading in the gap [1, 3].
        ASSERT_NO_FATAL_FAILURE(validate_reader_exactly(*reader, i, "56789"));
    }
    // Also read around the gap. Starting below the gap, we should skip it.
    {
        auto reader = readable_seg.make_reader(version_id{1});
        ASSERT_NO_FATAL_FAILURE(validate_reader_exactly(*reader, 0, "0156789"));
    }
    {
        auto reader = readable_seg.make_reader(version_id{1});
        ASSERT_NO_FATAL_FAILURE(validate_reader_exactly(*reader, 1, "156789"));
    }
    // Starting past the gap, it shouldn't appear either.
    {
        auto reader = readable_seg.make_reader(version_id{1});
        ASSERT_NO_FATAL_FAILURE(validate_reader_exactly(*reader, 5, "56789"));
    }
    {
        auto reader = readable_seg.make_reader(version_id{1});
        ASSERT_NO_FATAL_FAILURE(validate_reader_exactly(*reader, 6, "6789"));
    }
}

// Test applying random gaps and reading from from the beginning of the file.
TEST_F(SegmentReaderTest, TestReadsWithRandomGaps) {
    const auto size = 100;
    const ss::sstring data = random_generators::gen_alphanum_string(size);
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());

    version_id vid{1};
    absl::btree_set<int> removed_pos;
    expectations_map_t expected_per_version;
    for (int i = 0; i < 10; i++) {
        // Generate a random gap and build the cumulative expected string, with
        // all gaps so far if we were to read from the beginning.
        auto gap_start = random_generators::get_int(size);
        auto gap_len = random_generators::get_int(size);
        for (int l = 0; l < gap_len; l++) {
            removed_pos.emplace(gap_start + l);
        }
        // Rebuild the expected string, removing all gaps.
        ss::sstring expected;
        for (int d = 0; d < data.size(); d++) {
            if (removed_pos.contains(d)) {
                continue;
            }
            expected.append(&data[d], 1);
        }
        // Now validate all the versions against their expected reads.
        expected_per_version[vid] = expected;
        readable_seg.mutable_gaps()->add(file_gap(gap_start, gap_len), vid);
        ASSERT_NO_FATAL_FAILURE(
          validate_all_versions_exactly(readable_seg, expected_per_version));

        vid++;
        if (expected == "") {
            break;
        }
    }
}

// Test applying random gaps and reading from different positions in the file.
TEST_F(SegmentReaderTest, TestRandomReadsWithGaps) {
    const ss::sstring data = "abcdefghijklmnopqrstuvwxyz";
    const auto size = data.size();
    iobuf buf;
    buf.append(data.data(), data.size());
    paging_file_->append(std::move(buf)).get();
    readable_segment readable_seg(paging_file_.get());

    version_id vid{1};
    absl::btree_set<int> removed_pos;
    expectations_map_t expected_per_version;
    for (int i = 0; i < 10; i++) {
        auto gap_start = random_generators::get_int(size);
        auto gap_len = random_generators::get_int(size);
        for (int l = 0; l < gap_len; l++) {
            removed_pos.emplace(gap_start + l);
        }
        // Rebuild the expected string, removing all gaps.
        ss::sstring expected;
        for (int d = 0; d < data.size(); d++) {
            if (removed_pos.contains(d)) {
                continue;
            }
            expected.append(&data[d], 1);
        }
        // Now validate all the versions so far.
        expected_per_version[vid] = expected;
        readable_seg.mutable_gaps()->add(file_gap(gap_start, gap_len), vid);
        for (const auto& [vid, full_expected_str] : expected_per_version) {
            // Read starting at every position, and ensure nothing funny.
            for (int i = 0; i < size; i++) {
                auto reader = readable_seg.make_reader(vid);
                auto stream = reader->make_stream(i);
                auto buf = stream.read_exactly(size).get();

                // If there is anything to read, it should be the tail of the
                // expected with-gaps string.
                const ss::sstring actual{buf.get(), buf.size()};
                ASSERT_TRUE(full_expected_str.ends_with(actual)) << fmt::format(
                  "{} not tail of {}", actual, full_expected_str);
            }
        }

        vid++;
        if (expected == "") {
            break;
        }
    }
}
