/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "storage/fwd.h"
#include "utils/uuid.h"

namespace cluster {

// TODO: going to fundamental.h?
using cluster_uuid = named_type<uuid_t, struct cluster_uuid_type>;

/**
 * Read the stored cluster uuid from the kvstore
 * \return Cluster UUID if stored in kvstore, empty otherwise
 */
std::optional<cluster_uuid> read_stored_cluster_uuid(storage::api& storage);

ss::future<>
write_stored_cluster_uuid(storage::api& storage, const cluster_uuid&);

} // namespace cluster
