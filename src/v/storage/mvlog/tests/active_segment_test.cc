// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/units.h"
#include "model/tests/random_batch.h"
#include "random/generators.h"
#include "storage/directories.h"
#include "storage/mvlog/versioned_log.h"
#include "storage/ntp_config.h"
#include "test_utils/gtest_utils.h"

#include <seastar/core/seastar.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace storage::experimental::mvlog;
using namespace std::literals::chrono_literals;

class ActiveSegmentTest : public ::testing::Test {
public:
    void SetUp() override {
        base_dir_ = get_test_directory();
        storage::directories::initialize(base_dir_).get();
    }

    void TearDown() override {
        log_->close().get();
        log_.reset();
        std::filesystem::remove_all(std::filesystem::path(base_dir_));
    }
    versioned_log* make_log(
      std::optional<size_t> segment_bytes,
      tristate<std::chrono::milliseconds> segment_ms) {
        auto overrides
          = std::make_unique<storage::ntp_config::default_overrides>();
        overrides->segment_size = segment_bytes;
        overrides->segment_ms = segment_ms;
        storage::ntp_config cfg(model::ntp{}, base_dir_, std::move(overrides));
        log_ = std::make_unique<versioned_log>(std::move(cfg));
        return log_.get();
    }

    ss::future<ss::circular_buffer<model::record_batch>>
    write_random_batches(versioned_log* log, int num_batches) {
        auto in_batches = model::test::make_random_batches(
                            model::offset{0}, num_batches, true)
                            .get();
        for (const auto& b : in_batches) {
            co_await log->append(b.copy());
        }
        co_return in_batches;
    }

private:
    ss::sstring base_dir_;
    std::unique_ptr<versioned_log> log_;
};

TEST_F(ActiveSegmentTest, TestAppend) {
    auto* log = make_log(128_MiB, tristate<std::chrono::milliseconds>{});
    ASSERT_EQ(0, log->segment_count());

    write_random_batches(log, 10).get();
    ASSERT_EQ(1, log->segment_count());
}

TEST_F(ActiveSegmentTest, TestAppendPastLimit) {
    auto* log = make_log(128, tristate<std::chrono::milliseconds>{});

    // Each append should write to its own segment because of how small the
    // segment size is.
    write_random_batches(log, 10).get();
    ASSERT_EQ(10, log->segment_count());
}

TEST_F(ActiveSegmentTest, TestSegmentMs) {
    auto* log = make_log(128_MiB, tristate<std::chrono::milliseconds>{10ms});
    log->apply_segment_ms().get();
    ASSERT_EQ(0, log->segment_count());

    // Once we write, the countdown for rolling begins.
    write_random_batches(log, 1).get();
    ASSERT_EQ(1, log->segment_count());

    // Until we roll we should continue to have one segment.
    ss::sleep(20ms).get();
    ASSERT_EQ(1, log->segment_count());

    // Rolling will close the active segment.
    log->apply_segment_ms().get();
    ASSERT_FALSE(log->has_active_segment());
    ASSERT_EQ(1, log->segment_count());

    // Writing will always create a new segment.
    write_random_batches(log, 1).get();
    ASSERT_TRUE(log->has_active_segment());
    ASSERT_EQ(2, log->segment_count());
}

TEST_F(ActiveSegmentTest, TestAppendWithSegmentMs) {
    auto* log = make_log(128_MiB, tristate<std::chrono::milliseconds>{10ms});
    log->apply_segment_ms().get();
    ASSERT_EQ(0, log->segment_count());

    // Once we write, the countdown for rolling begins.
    write_random_batches(log, 1).get();
    ASSERT_EQ(1, log->segment_count());

    // Until we roll though, we should continue to have one segment.
    ss::sleep(20ms).get();
    ASSERT_EQ(1, log->segment_count());
    ASSERT_TRUE(log->has_active_segment());

    // An append past the rolling deadline shouldn't create a new segment.
    write_random_batches(log, 1).get();
    ASSERT_TRUE(log->has_active_segment());
    ASSERT_EQ(1, log->segment_count());

    // An explicit call to segment.ms will roll though.
    log->apply_segment_ms().get();
    ASSERT_FALSE(log->has_active_segment());
    ASSERT_EQ(1, log->segment_count());

    write_random_batches(log, 1).get();
    ASSERT_TRUE(log->has_active_segment());
    ASSERT_EQ(2, log->segment_count());
}

TEST_F(ActiveSegmentTest, TestApplySegmentMsEmpty) {
    auto* log = make_log(128_MiB, tristate<std::chrono::milliseconds>{10ms});
    log->apply_segment_ms().get();
    ASSERT_EQ(0, log->segment_count());

    // Even if we wait, there should be no rolls when applying segment.ms
    // because nothing has been written.
    ss::sleep(20ms).get();
    log->apply_segment_ms().get();
    log->apply_segment_ms().get();
    log->apply_segment_ms().get();
    ASSERT_EQ(0, log->segment_count());

    // The next write should create a single segment though (and shouldn't e.g.
    // process the previous attempts to apply segment.ms).
    write_random_batches(log, 1).get();
    ASSERT_EQ(1, log->segment_count());
    write_random_batches(log, 1).get();
    ASSERT_EQ(1, log->segment_count());
    write_random_batches(log, 1).get();
    ASSERT_EQ(1, log->segment_count());
}
