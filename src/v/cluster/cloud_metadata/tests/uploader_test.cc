/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "archival/ntp_archiver_service.h"
#include "cloud_storage/logger.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

class cluster_metadata_uploader_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        }).get();
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
    }
protected:
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

// Test that the upload fiber uploads monotonically increasing metadata, and
// that the fiber stop when leadership changes.
FIXTURE_TEST(test_upload_in_term, cluster_metadata_uploader_fixture) {
    auto raft
      = app.partition_manager.local().get(model::controller_ntp)->raft();
    config::shard_local_cfg()
      .cloud_storage_cluster_metadata_upload_interval_ms.set_value(1000ms);
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft);
    cluster::cloud_metadata::cluster_metadata_id highest_meta_id{0};
    for (int i = 0; i < 3; ++i) {
        // Wait to become leader before uploading.
        tests::cooperative_spin_wait_with_timeout(5s, [&raft] {
            return raft->is_leader();
        }).get();

        // Start uploading in this term.
        auto upload_in_term = uploader.upload_until_term_change();
        auto defer = ss::defer([&] {
            uploader.stop_and_wait().get();
            upload_in_term.get();
        });

        // Keep checking the latest manifest for whether the metadata ID is some
        // non-zero value (indicating we've uploaded multiple manifests);
        auto initial_meta_id = highest_meta_id;
        tests::cooperative_spin_wait_with_timeout(10s, [&] () -> ss::future<bool> {
            auto manifest = co_await uploader.download_or_create_manifest();
            if (!manifest.has_value()) {
                co_return false;
            }
            if (
              manifest->metadata_id() < initial_meta_id() + 1) {
                co_return false;
            }
            BOOST_REQUIRE_GE(manifest->metadata_id, highest_meta_id);
            highest_meta_id = manifest->metadata_id;
            co_await raft->step_down("forced stepdown");
            co_return true;
        }).get();
        upload_in_term.get();
        defer.cancel();
    }
}

// Basic check for downloading the latest manifest. The manifest downloaded by
// the uploader should be the one with the highest metadata ID.
FIXTURE_TEST(test_sync_manifest, cluster_metadata_uploader_fixture) {
    auto partition = app.partition_manager.local().get(model::controller_ntp);
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, partition->raft());

    // First try download when there's nothing in the bucket, e.g. as if it's
    // the first time we're using the bucket for this cluster.
    auto m_opt = uploader.download_or_create_manifest().get();
    BOOST_REQUIRE(m_opt.has_value());
    auto& m = m_opt.value();
    BOOST_CHECK_EQUAL(cluster_uuid, m.cluster_uuid());
    BOOST_CHECK_EQUAL(
      cluster::cloud_metadata::cluster_metadata_id{}, m.metadata_id);
    BOOST_CHECK_EQUAL(model::offset{}, m.controller_snapshot_offset);

    // Every manifest upload thereafter should lead a subsequent sync to
    // download the latest metadata, even if the manifests are left around.
    ss::abort_source as;
    retry_chain_node retry_node(as, ss::lowres_clock::time_point::max(), 10ms);
    cluster::cloud_metadata::cluster_metadata_manifest manifest;
    manifest.cluster_uuid = cluster_uuid;
    for (int i = 0; i < 11; i++) {
        manifest.metadata_id = cluster::cloud_metadata::cluster_metadata_id(i);
        // As a sanity check, upload directly rather than using uploader APIs.
        remote
          .upload_manifest(
            cloud_storage_clients::bucket_name("test-bucket"),
            manifest,
            retry_node)
          .get();

        // Syncing the manifest should always yield the manifest with the
        // highest metadata ID.
        auto m_opt = uploader.download_or_create_manifest().get();
        BOOST_REQUIRE(m_opt.has_value());
        BOOST_CHECK_EQUAL(i, m_opt->metadata_id());
    }

    // Now set the upload interval to something very low, which induces a low
    // timeout for downloads.
    config::shard_local_cfg()
      .cloud_storage_cluster_metadata_upload_interval_ms.set_value(0ms);
    m_opt = uploader.download_or_create_manifest().get();
    BOOST_CHECK(!m_opt.has_value());
}

