// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/versioned_gaps.h"

#include <gtest/gtest.h>

using namespace storage::experimental::mvlog;

TEST(VersionedGapListTest, TestEmpty) {
    versioned_gap_list versioned_gaps;
    auto gaps = versioned_gaps.gaps_up_to_including(version_id{0});
    ASSERT_TRUE(gaps.empty());

    // An empty gap does not get added.
    versioned_gaps.add(file_gap{0, 0}, version_id{1});
    ASSERT_TRUE(gaps.empty());
}

TEST(VersionedGapListTest, TestGetGaps) {
    versioned_gap_list versioned_gaps;
    versioned_gaps.add(file_gap{0, 1}, version_id{1});
    versioned_gaps.add(file_gap{5, 1}, version_id{10});

    auto gaps = versioned_gaps.gaps_up_to_including(version_id{0});
    ASSERT_TRUE(gaps.empty());

    for (int i = 1; i < 9; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(1, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 1);
    }
    for (int i = 10; i < 20; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(2, gaps.size());
        ASSERT_EQ(std::next(gaps.begin())->start, 5);
        ASSERT_EQ(std::next(gaps.begin())->end, 6);
    }
}

TEST(VersionedGapListTest, TestGetOverlappingGaps) {
    versioned_gap_list versioned_gaps;
    // Make some overlapping intervals.
    versioned_gaps.add(file_gap{0, 1}, version_id{1});
    versioned_gaps.add(file_gap{0, 10}, version_id{10});

    // Make some perfectly adjacent intervals.
    versioned_gaps.add(file_gap{20, 10}, version_id{20});
    versioned_gaps.add(file_gap{30, 10}, version_id{30});
    auto gaps = versioned_gaps.gaps_up_to_including(version_id{0});
    ASSERT_TRUE(gaps.empty());

    for (int i = 1; i < 9; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(1, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 1);
    }
    for (int i = 10; i < 20; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(1, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 10);
    }
    for (int i = 20; i < 30; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(2, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 10);
        ASSERT_EQ(std::next(gaps.begin())->start, 20);
        ASSERT_EQ(std::next(gaps.begin())->end, 30);
    }
    for (int i = 30; i < 40; i++) {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{i});
        ASSERT_EQ(2, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 10);
        ASSERT_EQ(std::next(gaps.begin())->start, 20);
        ASSERT_EQ(std::next(gaps.begin())->end, 40);
    }
}

TEST(VersionedGapListTest, TestAddUnorderedGaps) {
    versioned_gap_list versioned_gaps;
    versioned_gaps.add(file_gap{5, 1}, version_id{1});
    versioned_gaps.add(file_gap{7, 1}, version_id{2});
    versioned_gaps.add(file_gap{0, 10}, version_id{3});
    {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{0});
        ASSERT_TRUE(gaps.empty());
    }
    {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{1});
        ASSERT_EQ(1, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 5);
        ASSERT_EQ(gaps.begin()->end, 6);
    }
    {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{2});
        ASSERT_EQ(2, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 5);
        ASSERT_EQ(gaps.begin()->end, 6);
        ASSERT_EQ(std::next(gaps.begin())->start, 7);
        ASSERT_EQ(std::next(gaps.begin())->end, 8);
    }
    {
        auto gaps = versioned_gaps.gaps_up_to_including(version_id{3});
        ASSERT_EQ(1, gaps.size());
        ASSERT_EQ(gaps.begin()->start, 0);
        ASSERT_EQ(gaps.begin()->end, 10);
    }
}
TEST(VersionedGapListTest, TestAddUnorderedVersions) {
    versioned_gap_list versioned_gaps;
    versioned_gaps.add(file_gap{0, 10}, version_id{3});
    EXPECT_DEATH(
      versioned_gaps.add(file_gap{7, 1}, version_id{2}),
      "Expected monotonically increasing versions");
}
