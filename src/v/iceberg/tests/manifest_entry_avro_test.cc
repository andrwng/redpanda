// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/manifest_entry_avro.h"

#include <avro/Schema.hh>
#include <avro/ValidSchema.hh>
#include <gtest/gtest.h>

using namespace iceberg;

TEST(ManifestEntrySchemaTest, TestValidSchema) {
    // auto schema = avro::ValidSchema(manifest_entry_schema());
    // ASSERT_NO_FATAL_FAILURE(schema.toJson(true));
}
