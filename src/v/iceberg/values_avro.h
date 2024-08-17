// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/datatypes.h"
#include "iceberg/values.h"

namespace iceberg {

// Serializes the given struct value with the given struct type as an Avro
// iobuf. The given struct name will be included in the Avro schema, and the
// given metadata will be included in the Avro header.
//
// XXX: only use this for Iceberg manifest metadata! Not all Avro types are
// implemented yet.
iobuf struct_to_avro_buf(
  const struct_value&,
  const struct_type&,
  const ss::sstring& struct_name,
  std::map<std::string, std::string> metadata = {});

} // namespace iceberg
