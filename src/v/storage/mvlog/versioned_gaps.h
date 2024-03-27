// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "container/interval_set.h"
#include "storage/mvlog/file_gap.h"
#include "storage/mvlog/version_id.h"

#include <cstddef>

namespace storage::experimental::mvlog {

class versioned_gap_list {
public:
    // Expects the gap to be added to be higher than any gap previously added.
    void add(file_gap, version_id);

    // Returns the set of gaps added up to and including the given
    // version_id.
    interval_set<size_t> gaps_up_to_including(version_id inclusive_id) const;

private:
    struct versioned_gap {
        version_id vid;
        file_gap gap;
    };
    chunked_vector<versioned_gap> gaps_;
};

} // namespace storage::experimental::mvlog
