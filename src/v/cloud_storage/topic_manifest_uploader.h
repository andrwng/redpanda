// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cloud_storage/topic_path_provider.h"
#include "cluster/topic_configuration.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"
#include "utils/retry_chain_node.h"

#include <fmt/format.h>

#include <expected>

namespace cloud_storage {

class remote;

class topic_manifest_uploader {
public:
    enum class errc {
        io_error,
        shutting_down,
    };
    using error = detailed_error<errc>;

    topic_manifest_uploader(
      ss::logger&,
      const cloud_storage_clients::bucket_name& bucket,
      cloud_storage::remote&);

    ss::future<std::expected<void, error>> upload_manifest(
      const topic_path_provider& path_provider,
      const cluster::topic_configuration& cfg,
      model::initial_revision_id rev,
      retry_chain_node& retry_node);

private:
    ss::logger& log_;
    const cloud_storage_clients::bucket_name bucket_;
    cloud_storage::remote& remote_;
};

} // namespace cloud_storage

template<>
struct fmt::formatter<cloud_storage::topic_manifest_uploader::errc>
  : fmt::formatter<std::string_view> {
    auto format(
      cloud_storage::topic_manifest_uploader::errc e,
      fmt::format_context& ctx) const {
        using errc = cloud_storage::topic_manifest_uploader::errc;
        std::string_view name;
        switch (e) {
        case errc::io_error:
            name = "io_error";
            break;
        case errc::shutting_down:
            name = "shutting_down";
            break;
        }
        return fmt::format_to(ctx.out(), "{}", name);
    }
};
