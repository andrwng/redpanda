/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/uploader.h"

#include "cloud_storage/remote.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/logger.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "redpanda/admin/api-doc/cluster_config.json.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

namespace {

const std::regex cluster_metadata_manifest_expr{
  R"REGEX(/cluster_metadata/[a-z0-9-]+/manifests/(\d+)/)REGEX"};

} // anonymous namespace

namespace cluster::cloud_metadata {

uploader::uploader(
  model::cluster_uuid cluster_uuid,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  consensus_ptr raft0)
  : _cluster_uuid(cluster_uuid)
  , _remote(remote)
  , _raft0(std::move(raft0))
  , _bucket(bucket)
  , _upload_interval_ms(config::shard_local_cfg()
                          .cloud_storage_cluster_metadata_upload_interval_ms.bind()) {}

ss::future<bool> uploader::term_has_changed(model::term_id term) {
    if (!_raft0->is_leader() || _raft0->term() != term) {
        co_return true;
    }
    auto barrier = co_await _raft0->linearizable_barrier();
    if (!barrier.has_value()) {
        co_return true;
    }
    // Following the above barrier, we're a healthy leader. Make sure our term
    // didn't change while linearizing.
    if (!_raft0->is_leader() || _raft0->term() != term) {
        co_return true;
    }
    co_return false;
}

ss::future<> uploader::upload_until_term_change() {
    ss::gate::holder g(_gate);
    if (!_raft0->is_leader()) {
        vlog(
          clusterlog.trace,
          "Not the leader, exiting uploader");
        co_return;
    }
    // Take care to ensure the optional<> is only set for as long as the
    // reference is valid.
    ss::abort_source term_as;
    _term_as = term_as;
    auto reset_term_as = ss::defer([this] { _term_as.reset(); });
    // Since this loop isn't driven by a Raft STM, the uploader doesn't have a
    // long-lived in-memory manifest that it keeps up-to-date: It's possible
    // that an uploader from a different node uploaded since last time this
    // replica was leader. As such, every time we change terms, we need to
    // re-sync the manifest.
    auto synced_term = _raft0->term();
    vlog(
      clusterlog.info,
      "Syncing cluster metadata manifest in term {}",
      synced_term);
    auto manifest_opt = co_await download_or_create_manifest();
    if (!manifest_opt.has_value()) {
        vlog(
          clusterlog.warn,
          "Manifest download failed in term {}", synced_term);
        co_return;
    }
    vlog(
      clusterlog.info,
      "Starting cluster metadata upload loop in term {}",
      synced_term);

    auto& manifest = manifest_opt.value();
    while (_raft0->is_leader() && _raft0->term() == synced_term) {
        // TODO: check if we need to upload a new controller snapshot.

        if (co_await term_has_changed(synced_term)) {
            co_return;
        }
        vlog(
          clusterlog.trace,
          "Controller is leader in term {}",
          synced_term);

        // TODO: upload the controller snapshot.

        // Upload a fresh manifest if we can.
        if (co_await term_has_changed(synced_term)) {
            co_return;
        }
        if (manifest.metadata_id == cluster_metadata_id{}) {
            manifest.metadata_id = cluster_metadata_id(0);
        } else {
            manifest.metadata_id = cluster_metadata_id(
              manifest.metadata_id() + 1);
        }
        manifest.upload_time_since_epoch
          = ss::lowres_clock::now().time_since_epoch();
        retry_chain_node retry_node(_as, _upload_interval_ms(), 100ms);
        auto upload_result = co_await _remote.upload_manifest(
          _bucket, manifest, retry_node);
        if (upload_result != cloud_storage::upload_result::success) {
            vlog(
              clusterlog.warn,
              "Failed to upload cluster metadata manifest in term {}: {}",
              synced_term,
              upload_result);
        }

        // TODO: clean up older manifests.

        try {
            co_await ssx::sleep_abortable(_upload_interval_ms(), _as, term_as);
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
    }
}

ss::future<std::optional<cluster_metadata_manifest>>
uploader::download_or_create_manifest() {
    // Download the manifest
    retry_chain_node retry_node(_as, _upload_interval_ms(), 100ms);
    auto cluster_uuid_prefix = "/" + cluster_manifests_prefix(_cluster_uuid) + "/";
    vlog(
      clusterlog.trace, "Listing objects with prefix {}", cluster_uuid_prefix);
    auto list_res = co_await _remote.list_objects(
      _bucket,
      retry_node,
      cloud_storage_clients::object_key(cluster_uuid_prefix),
      '/');
    if (list_res.has_error()) {
        vlog(
          clusterlog.debug, "Error downloading manifest", list_res.error());
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
          _cluster_uuid());
        // There are no existing manifests. Create a new one.
        manifest.cluster_uuid = _cluster_uuid;
        co_return manifest;
    }
    for (const auto& prefix : manifest_prefixes) {
        vlog(
          clusterlog.trace,
          "Prefix found for {}: {}",
          _cluster_uuid(),
          prefix);
    }
    // Find the manifest with the highest metadata ID.
    cluster_metadata_id highest_meta_id;
    for (const auto& prefix : manifest_prefixes) {
        std::smatch matches;
        std::string p = prefix;
        // E.g. /cluster_metadata_<cluster_uuid>_manifests/3/
        const auto matches_manifest_expr = std::regex_match(
          p.cbegin(), p.cend(), matches, cluster_metadata_manifest_expr);
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
        manifest.cluster_uuid = _cluster_uuid;
        co_return manifest;
    }

    // Deserialize the manifest.
    auto manifest_res = co_await _remote.download_manifest(
      _bucket,
      cluster_manifest_key(_cluster_uuid, highest_meta_id),
      manifest,
      retry_node);
    if (manifest_res != cloud_storage::download_result::success) {
        vlog(
          clusterlog.debug,
          "Manifest download failed with {}",
          manifest_res);
        co_return std::nullopt;
    }
    co_return manifest;
}

ss::future<> uploader::stop_and_wait() {
    _as.request_abort();
    co_await _gate.close();
}

} // namespace cluster::cloud_metadata
