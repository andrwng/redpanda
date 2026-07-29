/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/bytes.h"
#include "datalake/coordinator/data_file.h"
#include "datalake/coordinator/translated_offset_range.h"

#include <gtest/gtest.h>

using namespace datalake::coordinator;

namespace {
data_file make_file_with_stats(size_t bound_bytes) {
    data_file f;
    f.remote_path = "path.parquet";
    datalake::per_column_stats cs;
    cs.field_id = 1;
    cs.lower_bound = bytes::from_string(std::string(bound_bytes, 'a'));
    cs.upper_bound = bytes::from_string(std::string(bound_bytes, 'z'));
    f.column_stats.push_back(std::move(cs));
    return f;
}
} // namespace

// A wider stat payload must be accounted as more memory: the per-column bounds
// are the term that varies across schemas.
TEST(EstimatedMemoryBytes, StatBoundsDominate) {
    auto narrow = estimated_memory_bytes(make_file_with_stats(8));
    auto wide = estimated_memory_bytes(make_file_with_stats(4096));
    EXPECT_GT(wide, narrow);
    // Two 4096-byte bounds must be visible in the estimate.
    EXPECT_GE(wide - narrow, 2 * (4096 - 8));
}

// A file with no column stats still costs its path plus struct overhead.
TEST(EstimatedMemoryBytes, NoStats) {
    data_file f;
    f.remote_path = "some/longer/path/to/a/file.parquet";
    EXPECT_GE(estimated_memory_bytes(f), f.remote_path.size());
}

// A range's estimate is the sum over its main and DLQ files.
TEST(EstimatedMemoryBytes, RangeSumsMainAndDlq) {
    translated_offset_range r;
    r.files.push_back(make_file_with_stats(16));
    r.dlq_files.push_back(make_file_with_stats(16));
    auto per_file = estimated_memory_bytes(make_file_with_stats(16));
    EXPECT_GE(estimated_memory_bytes(r), 2 * per_file);
}
