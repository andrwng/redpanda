/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/cloud_metadata/consumer_offsets_types.h"

namespace cluster::cloud_metadata {

// Interface to run a recovery of the given requested groups.
class offsets_recovery_manager {
public:
  virtual ss::future<offsets_recovery_reply> recover_offsets(offsets_recovery_request) = 0;
};

} // namespace cluster::cloud_metadata
