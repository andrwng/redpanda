// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/chunked_hash_map.h"
#include "iceberg/datatypes.h"
#include "iceberg/schema.h"

namespace iceberg {

using ids_types_map_t = chunked_hash_map<nested_field::id_t, const field_type*>;
ids_types_map_t ids_to_types(const schema&);

} // namespace iceberg
