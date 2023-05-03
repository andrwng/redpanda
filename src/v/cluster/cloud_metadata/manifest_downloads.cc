/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/manifest_downloads.h"

#include "cloud_storage/remote.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/logger.h"
#include "utils/uuid.h"

#include <boost/uuid/uuid_io.hpp>

namespace {

const std::regex cluster_metadata_manifest_prefix_expr{
  R"REGEX(/cluster_metadata/[a-z0-9-]+/manifests/(\d+)/)REGEX"};

const std::regex cluster_metadata_manifest_expr{
  R"REGEX(/cluster_metadata/([a-z0-9-]+)/manifests/(\d+)/cluster_manifest.json)REGEX"};

} // anonymous namespace

namespace cluster::cloud_metadata {

ss::future<std::optional<cluster_metadata_manifest>>
download_or_create_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  retry_chain_node& retry_node) {
    // Download the manifest
    auto cluster_uuid_prefix = "/" + cluster_manifests_prefix(cluster_uuid)
                               + "/";
    vlog(
      clusterlog.trace, "Listing objects with prefix {}", cluster_uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_uuid_prefix),
      '/');
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug, "Error downloading manifest {}", list_res.error());
        co_return std::nullopt;
    }
    // Examine the metadata IDs for this cluster.
    // Results take the form:
    // "cluster_metadata_manifests_<cluster_uuid>/<meta_id>/"
    auto& manifest_prefixes = list_res.value().common_prefixes;
    cluster_metadata_manifest manifest;
    if (manifest_prefixes.empty()) {
        vlog(
          clusterlog.debug,
          "No manifests found for cluster {}, creating new one",
          cluster_uuid());
        // There are no existing manifests. Create a new one.
        manifest.cluster_uuid = cluster_uuid;
        co_return manifest;
    }
    for (const auto& prefix : manifest_prefixes) {
        vlog(
          clusterlog.trace, "Prefix found for {}: {}", cluster_uuid(), prefix);
    }
    // Find the manifest with the highest metadata ID.
    cluster_metadata_id highest_meta_id;
    for (const auto& prefix : manifest_prefixes) {
        std::smatch matches;
        std::string p = prefix;
        // E.g. /cluster_metadata_<cluster_uuid>_manifests/3/
        const auto matches_manifest_expr = std::regex_match(
          p.cbegin(), p.cend(), matches, cluster_metadata_manifest_prefix_expr);
        if (!matches_manifest_expr) {
            continue;
        }
        const auto& meta_id_str = matches[1].str();
        cluster_metadata_id meta_id;
        try {
            meta_id = cluster_metadata_id(std::stoi(meta_id_str.c_str()));
        } catch (...) {
            vlog(
              clusterlog.debug,
              "Ignoring invalid metadata ID: {}",
              meta_id_str);
            continue;
        }
        highest_meta_id = std::max(highest_meta_id, meta_id);
    }
    if (highest_meta_id == cluster_metadata_id{}) {
        // There are no existing manifests. Create a new one.
        vlog(
          clusterlog.debug,
          "No valid manifests found for cluster {}, creating new one",
          cluster_uuid());
        manifest.cluster_uuid = cluster_uuid;
        co_return manifest;
    }

    // Deserialize the manifest.
    auto manifest_res = co_await remote.download_manifest(
      bucket,
      cluster_manifest_key(cluster_uuid, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug, "Manifest download failed with {}", manifest_res);
        co_return std::nullopt;
    }
    vlog(
      clusterlog.trace,
      "Downloaded manifest for {} from {}: {}",
      cluster_uuid(),
      bucket(),
      manifest);
    co_return manifest;
}

ss::future<std::list<ss::sstring>> list_orphaned_by_manifest(
  cloud_storage::remote& remote,
  const model::cluster_uuid& cluster_uuid,
  const cloud_storage_clients::bucket_name& bucket,
  const cluster_metadata_manifest& manifest,
  retry_chain_node& retry_node) {
    auto uuid_prefix = "/" + cluster_uuid_prefix(cluster_uuid) + "/";
    vlog(clusterlog.trace, "Listing objects with prefix {}", uuid_prefix);
    auto list_res = co_await remote.list_objects(
      bucket, retry_node, cloud_storage_clients::object_key(uuid_prefix), '/');
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug,
          "Error listing under {}: {}",
          uuid_prefix,
          list_res.error());
        co_return std::list<ss::sstring>{};
    }
    std::list<ss::sstring> ret;
    for (auto& item : list_res.value().contents) {
        if (
          item.key == ss::sstring{manifest.get_manifest_path()()}
          || item.key == manifest.controller_snapshot_path) {
            continue;
        }
        ret.emplace_back(std::move(item.key));
    }
    co_return ret;
}

} // namespace cluster::cloud_metadata
