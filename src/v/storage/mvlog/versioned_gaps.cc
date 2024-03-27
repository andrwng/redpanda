// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/versioned_gaps.h"

#include "base/vlog.h"
#include "container/interval_set.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/version_id.h"

namespace storage::experimental::mvlog {

void versioned_gap_list::add(file_gap gap, version_id vid) {
    vassert(
      gaps_.empty() || gaps_.back().first < vid,
      "Expected monotonically increasing versions {} < {}",
      gaps_.back().first,
      vid);
    if (gap.length <= 0) {
        return;
    }
    vlog(
      log.trace,
      "Adding gap [{}, {}) for version {}",
      gap.start_pos,
      gap.start_pos + gap.length,
      vid);
    gaps_.emplace_back(vid, gap);
}

interval_set<size_t>
versioned_gap_list::gaps_up_to_including(version_id inclusive_id) const {
    interval_set<size_t> ret;
    for (const auto& [gap_vid, gap] : gaps_) {
        if (gap_vid > inclusive_id) {
            break;
        }
        vlog(
          log.trace,
          "Returning gap [{}, {}), version {} <= {}",
          gap.start_pos,
          gap.start_pos + gap.length,
          gap_vid,
          inclusive_id);
        auto [_, inserted] = ret.insert({gap.start_pos, gap.length});
        vassert(inserted, "Failed to add to interval");
    }
    return ret;
}

} // namespace storage::experimental::mvlog
