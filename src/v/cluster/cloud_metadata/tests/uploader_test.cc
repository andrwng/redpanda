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
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/cloud_metadata/uploader.h"
#include "cluster/config_frontend.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/application.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

#include <math.h>

namespace {
ss::logger logger("uploader_test");
} // anonymous namespace

class cluster_metadata_uploader_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    cluster_metadata_uploader_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{}, httpd_port_number())
      , raft0(app.partition_manager.local().get(model::controller_ntp)->raft())
      , controller_stm(app.controller->get_controller_stm().local())
      , remote(app.cloud_storage_api.local())
      , bucket(cloud_storage_clients::bucket_name("test-bucket")) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        tests::cooperative_spin_wait_with_timeout(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        }).get();
        cluster_uuid = app.storage.local().get_cluster_uuid().value();
        app.feature_table.local().testing_activate_feature(
          features::feature::controller_snapshots);
    }

    ss::future<
      std::optional<cluster::cloud_metadata::cluster_metadata_manifest>>
    download_or_create_manifest(
      const ss::lowres_clock::time_point& deadline
      = ss::lowres_clock::time_point::max()) {
        ss::abort_source as;
        retry_chain_node retry_node(as, deadline, 10ms);
        co_return co_await cluster::cloud_metadata::download_or_create_manifest(
          remote, cluster_uuid, bucket, retry_node);
    }

    // Returns true if the manifest downloaded has a higher metadata ID than
    // `initial_meta_id`.
    ss::future<bool> downloaded_manifest_has_higher_id(
      cluster::cloud_metadata::cluster_metadata_id initial_meta_id,
      cluster::cloud_metadata::cluster_metadata_manifest* downloaded_manifest) {
        auto manifest = co_await download_or_create_manifest();
        if (!manifest.has_value()) {
            co_return false;
        }
        if (manifest->metadata_id() <= initial_meta_id()) {
            co_return false;
        }
        *downloaded_manifest = std::move(manifest.value());
        co_return true;
    }

protected:
    cluster::consensus_ptr raft0;
    cluster::controller_stm& controller_stm;
    cloud_storage::remote& remote;
    const cloud_storage_clients::bucket_name bucket;
    model::cluster_uuid cluster_uuid;
};

// Test that the upload fiber uploads monotonically increasing metadata, and
// that the fiber stop when leadership changes.
FIXTURE_TEST(test_upload_in_term, cluster_metadata_uploader_fixture) {
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return controller_stm.maybe_write_snapshot();
    }).get();
    const auto get_local_snap_offset = [&] {
        auto snap = raft0->open_snapshot().get();
        BOOST_REQUIRE(snap.has_value());
        auto ret = snap->metadata.last_included_index;
        snap->close().get();
        return ret;
    };
    const auto snap_offset = get_local_snap_offset();

    config::shard_local_cfg()
      .cloud_storage_cluster_metadata_upload_interval_ms.set_value(1000ms);
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);
    cluster::cloud_metadata::cluster_metadata_id highest_meta_id{0};

    // Checks that metadata is uploaded a new term, stepping down in between
    // calls, and ensuring that subsequent calls yield manifests with higher
    // metadata IDs and the expected snapshot offset.
    const auto check_uploads_in_term_and_stepdown =
      [&](model::offset expected_snap_offset) {
          // Wait to become leader before uploading.
          tests::cooperative_spin_wait_with_timeout(5s, [this] {
              return raft0->is_leader();
          }).get();

          // Start uploading in this term.
          auto upload_in_term = uploader.upload_until_term_change();
          auto defer = ss::defer([&] {
              uploader.stop_and_wait().get();
              upload_in_term.get();
          });

          // Keep checking the latest manifest for whether the metadata ID is
          // some non-zero value (indicating we've uploaded multiple manifests);
          auto initial_meta_id = highest_meta_id;
          cluster::cloud_metadata::cluster_metadata_manifest manifest;
          tests::cooperative_spin_wait_with_timeout(
            10s,
            [&]() -> ss::future<bool> {
                return downloaded_manifest_has_higher_id(
                  initial_meta_id, &manifest);
            })
            .get();
          BOOST_REQUIRE_GT(manifest.metadata_id, highest_meta_id);
          highest_meta_id = manifest.metadata_id;

          BOOST_REQUIRE_EQUAL(
            manifest.controller_snapshot_offset, expected_snap_offset);

          // Stop the upload loop and continue in a new term.
          raft0->step_down("forced stepdown").get();
          upload_in_term.get();
          defer.cancel();
      };
    for (int i = 0; i < 3; ++i) {
        check_uploads_in_term_and_stepdown(snap_offset);
    }

    // Now do some action and write a new snapshot.
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return raft0->is_leader();
    }).get();
    auto result = app.controller->get_config_frontend()
                    .local()
                    .do_patch(
                      cluster::config_update_request{
                        .upsert = {{"cluster_id", "foo"}}},
                      model::timeout_clock::now() + 5s)
                    .get();
    BOOST_REQUIRE(!result.errc);
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return controller_stm.maybe_write_snapshot();
    }).get();
    const auto new_snap_offset = get_local_snap_offset();
    BOOST_REQUIRE_NE(new_snap_offset, snap_offset);
    check_uploads_in_term_and_stepdown(new_snap_offset);
}

// Basic check for downloading the latest manifest. The manifest downloaded by
// the uploader should be the one with the highest metadata ID.
FIXTURE_TEST(test_download_manifest, cluster_metadata_uploader_fixture) {
    cluster::cloud_metadata::uploader uploader(
      cluster_uuid, bucket, remote, raft0);

    // First try download when there's nothing in the bucket, e.g. as if it's
    // the first time we're using the bucket for this cluster.
    auto m_opt = download_or_create_manifest().get();
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

        // Downloading the manifest should always yield the manifest with the
        // highest metadata ID.
        auto m_opt = download_or_create_manifest().get();
        BOOST_REQUIRE(m_opt.has_value());
        BOOST_CHECK_EQUAL(i, m_opt->metadata_id());
    }

    // Now set the deadline to something very low, and check that it fails.
    m_opt
      = download_or_create_manifest(ss::lowres_clock::time_point::min()).get();
    BOOST_CHECK(!m_opt.has_value());
}
