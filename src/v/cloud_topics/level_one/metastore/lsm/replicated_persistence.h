/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

// Open a metadata persistence object in the given bucket at the prefix. In
// addition to writing to cloud, this metadata persistence replicates the
// manifest via the replicated state machine.
ss::future<std::unique_ptr<lsm::io::metadata_persistence>>
open_replicated_metadata_persistence(
  stm* stm,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix);

} // namespace cloud_topics::l1
