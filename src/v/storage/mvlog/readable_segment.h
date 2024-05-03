// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "storage/mvlog/version_id.h"
#include "storage/mvlog/versioned_gaps.h"

#include <memory>

namespace storage::experimental::mvlog {

class file;
class segment_reader;

// A readable segment file. This is a long-lived object, responsible for
// passing out short-lived readers.
class readable_segment {
public:
    explicit readable_segment(file* f)
      : file_(f) {}

    std::unique_ptr<segment_reader> make_reader(const version_id tid);
    size_t num_readers() const { return num_readers_; }
    const versioned_gap_list& gaps() const { return gaps_; }
    versioned_gap_list* mutable_gaps() { return &gaps_; }

private:
    friend class segment_reader;

    file* file_;

    // TODO(awong): these could be lazily materialized.
    versioned_gap_list gaps_;

    size_t num_readers_{0};
};

} // namespace storage::experimental::mvlog
