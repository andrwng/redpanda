// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/topic_manifest_uploader.h"

#include "cloud_storage/remote.h"
#include "cloud_storage/topic_manifest.h"
#include "ssx/future-util.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_storage {

topic_manifest_uploader::topic_manifest_uploader(
  ss::logger& log,
  const cloud_storage_clients::bucket_name& bucket,
  cloud_storage::remote& remote)
  : log_(log)
  , bucket_(bucket)
  , remote_(remote) {}

ss::future<std::expected<void, topic_manifest_uploader::error>>
topic_manifest_uploader::upload_manifest(
  const topic_path_provider& path_provider,
  const cluster::topic_configuration& cfg,
  model::initial_revision_id rev,
  retry_chain_node& retry_node) {
    topic_manifest manifest(cfg, rev);
    auto key = manifest.get_manifest_path(path_provider);
    vlog(log_.debug, "Uploading topic manifest to '{}': {}", key, cfg);

    auto res = co_await ss::coroutine::as_future(
      remote_.upload_manifest(bucket_, manifest, key, retry_node));

    if (res.failed()) {
        auto ex = res.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(
              error(errc::shutting_down, "Shutdown exception: {}", ex));
        }
        co_return std::unexpected(
          error(errc::io_error, "Upload exception: {}", ex));
    }

    if (res.get() != upload_result::success) {
        co_return std::unexpected(
          error(errc::io_error, "Upload failed: {}", key));
    }

    co_return std::expected<void, error>{};
}

} // namespace cloud_storage
