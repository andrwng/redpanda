// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "iceberg/values.h"

#include <avro/GenericDatum.hh>

namespace iceberg {

avro::GenericDatum value_as_avro(const value_ptr&, const avro::NodePtr&);
value value_from_avro(const avro::GenericDatum&);

} // namespace iceberg
