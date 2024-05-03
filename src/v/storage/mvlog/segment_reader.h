// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/mvlog/batch_collector.h"
#include "storage/mvlog/skipping_data_source.h"
#include "storage/mvlog/version_id.h"
#include "storage/types.h"

namespace storage::experimental::mvlog {

class readable_segment;

// Class that encapsulates read IO for a segment file.
class segment_reader {
public:
    segment_reader(segment_reader&) = delete;
    segment_reader(segment_reader&&) = delete;
    segment_reader& operator=(segment_reader&) = delete;
    segment_reader& operator=(segment_reader&&) = delete;

    explicit segment_reader(
      const version_id inclusive_id, readable_segment* segment);
    ~segment_reader();

    // Returns a stream starting from the given file position, until the end of
    // the segment file.
    //
    // The stream must be consumed while this segment_reader remains in scope.
    ss::input_stream<char> make_stream(size_t start_pos = 0) const;

private:
    // Returns the set of file position intervals appropriate for this reader's
    // version id.
    skipping_data_source::read_list_t
    make_read_intervals(size_t start_pos, size_t length) const;

    // Returns a stream starting at the file position with the given length.
    ss::input_stream<char> make_stream(size_t start_pos, size_t length) const;

    // The version id with which this reader is versioned. Only gaps
    // versioned with this id or lower are materialized by this reader.
    const version_id inclusive_id_;

    // The underlying readable segment file.
    readable_segment* segment_;
};

} // namespace storage::experimental::mvlog
