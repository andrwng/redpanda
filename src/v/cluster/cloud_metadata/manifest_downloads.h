/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/cloud_metadata/cluster_manifest.h"
#include "seastarx.h"

#include <seastar/core/future.hh>

namespace cloud_storage {
class remote;
} // namespace cloud_storage

class retry_chain_node;

namespace cluster::cloud_metadata {

// Downloads the manifest with the highest metadata ID for the cluster, or
// creates an empty manifest if no such manifest exists.
// Returns nullopt if download wasn't successful.
ss::future<std::optional<cluster_metadata_manifest>>
download_or_create_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node);

// Returns keys with the cluster UUID prefix aren't referenced by the manifest
// and can be safely deleted.
ss::future<std::list<ss::sstring>> list_orphaned_by_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  const cluster_metadata_manifest& manifest,
  retry_chain_node& retry_node);

// Looks through the given bucket for cluster metadata with the highest
// metadata ID.
// Returns nullopt if there are no manifests.
ss::future<std::optional<cluster_metadata_manifest>> find_latest_manifest(
  cloud_storage::remote& remote,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node);

} // namespace cluster::cloud_metadata
