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
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/logger.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"
#include "storage/snapshot.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/defer.hh>

#include <exception>

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
  , _upload_interval_ms(
      config::shard_local_cfg()
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
        vlog(clusterlog.trace, "Not the leader, exiting uploader");
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
    retry_chain_node retry_node(_as, _upload_interval_ms(), 100ms);
    auto manifest_opt = co_await download_or_create_manifest(
      _remote, _cluster_uuid, _bucket, retry_node);
    if (!manifest_opt.has_value()) {
        vlog(
          clusterlog.warn, "Manifest download failed in term {}", synced_term);
        co_return;
    }
    vlog(
      clusterlog.info,
      "Starting cluster metadata upload loop in term {}",
      synced_term);

    auto& manifest = manifest_opt.value();
    while (_raft0->is_leader() && _raft0->term() == synced_term) {
        if (co_await term_has_changed(synced_term)) {
            co_return;
        }
        vlog(clusterlog.trace, "Controller is leader in term {}", synced_term);

        auto controller_snap_file = co_await _raft0->open_snapshot_file();
        retry_chain_node retry_node(_as, _upload_interval_ms(), 100ms);

        // Update the in-memory metadata ID to prepare for further uploads in
        // this term.
        if (manifest.metadata_id == cluster_metadata_id{}) {
            manifest.metadata_id = cluster_metadata_id(0);
        } else {
            manifest.metadata_id = cluster_metadata_id(
              manifest.metadata_id() + 1);
        }

        // Set up an abort source for if there is a leadership change while
        // we're uploading.
        auto lazy_as = cloud_storage::lazy_abort_source{
          [&, synced_term]() {
              return synced_term != _raft0->term()
                       ? std::make_optional(fmt::format(
                         "lost leadership or term changed: synced term {} vs "
                         "current term {}",
                         synced_term,
                         _raft0->term()))
                       : std::nullopt;
          },
        };
        if (controller_snap_file.has_value()) {
            vlog(
              clusterlog.trace,
              "Local controller snapshot found at {}",
              _raft0->get_snapshot_path());
            auto reader = storage::snapshot_reader(
              controller_snap_file.value(),
              ss::make_file_input_stream(
                *controller_snap_file,
                0,
                co_await controller_snap_file->size()),
              _raft0->get_snapshot_path());
            model::offset local_last_included_offset;
            std::exception_ptr err;
            try {
                auto snap_metadata_buf = co_await reader.read_metadata();
                auto snap_parser = iobuf_parser(std::move(snap_metadata_buf));
                auto snap_metadata = reflection::adl<raft::snapshot_metadata>{}
                                       .from(snap_parser);
                local_last_included_offset = snap_metadata.last_included_index;
                vassert(
                  snap_metadata.last_included_index != model::offset{},
                  "Invalid offset for snapshot {}",
                  _raft0->get_snapshot_path());
                vlog(
                  clusterlog.debug,
                  "Local controller snapshot at {} has last offset {}, current "
                  "snapshot offset in manifest {}",
                  _raft0->get_snapshot_path(),
                  local_last_included_offset,
                  manifest.controller_snapshot_offset);

                // If we haven't uploaded a snapshot or the local snapshot is
                // new, upload it.
                if (
                  manifest.controller_snapshot_offset == model::offset{}
                  || local_last_included_offset
                       > manifest.controller_snapshot_offset) {
                    cloud_storage::remote_segment_path
                      remote_controller_snapshot_path{controller_snapshot_key(
                        _cluster_uuid, manifest.metadata_id)};
                    auto upl_res = co_await _remote.upload_file(
                      _bucket,
                      remote_controller_snapshot_path,
                      controller_snap_file.value(),
                      retry_node,
                      lazy_as);
                    if (upl_res == cloud_storage::upload_result::success) {
                        manifest.controller_snapshot_path
                          = remote_controller_snapshot_path().string();
                        manifest.controller_snapshot_offset
                          = local_last_included_offset;
                    }
                }
            } catch (...) {
                err = std::current_exception();
            }
            co_await reader.close();
            if (err) {
                std::rethrow_exception(err);
            }
        }

        // Upload a fresh manifest if we can.
        if (co_await term_has_changed(synced_term)) {
            co_return;
        }
        manifest.upload_time_since_epoch
          = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch());
        auto upload_result = co_await _remote.upload_manifest(
          _bucket, manifest, retry_node);
        if (upload_result != cloud_storage::upload_result::success) {
            vlog(
              clusterlog.warn,
              "Failed to upload cluster metadata manifest in term {}: {}",
              synced_term,
              upload_result);
        }

        // TODO: clean up older manifests and snapshots.
        // XXX fix retry node
        auto orphaned_by_manifest = co_await list_orphaned_by_manifest(
          _remote, _cluster_uuid, _bucket, manifest, retry_node);
        for (const auto& s : orphaned_by_manifest) {
            if (co_await term_has_changed(synced_term)) {
                co_return;
            }
            auto path = std::filesystem::path{s};
            auto res = co_await _remote.delete_object(
              _bucket, cloud_storage_clients::object_key{path}, retry_node);
            if (res != cloud_storage::upload_result::success) {
                vlog(
                  clusterlog.warn, "Failed to delete orphaned metadata: {}", s);
            }
        }

        try {
            co_await ssx::sleep_abortable(_upload_interval_ms(), _as, term_as);
        } catch (const ss::sleep_aborted&) {
            co_return;
        }
    }
}

ss::future<> uploader::stop_and_wait() {
    _as.request_abort();
    co_await _gate.close();
}

} // namespace cluster::cloud_metadata
