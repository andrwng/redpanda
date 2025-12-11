/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/remote.h"
#include "cloud_io/tests/s3_imposter.h"
#include "cloud_io/tests/scoped_remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "config/node_config.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "model/fundamental.h"
#include "raft/tests/raft_fixture.h"
#include "random/generators.h"
#include "test_utils/async.h"
#include "test_utils/scoped_config.h"
#include "test_utils/tmp_dir.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <functional>

using namespace cloud_topics::l1;

namespace {

ss::logger rdb_test_log("replicated_db_test");
cloud_storage_clients::object_key test_prefix{"test-prefix"};

ss::future<> count_rows(lsm::database& db, size_t& ret) {
    auto it = db.create_iterator().get();
    co_await it.seek_to_first();
    size_t rows_count = 0;
    while (it.valid()) {
        ++rows_count;
        co_await it.next();
    }
    ret = rows_count;
}

struct replicated_db_node {
    replicated_db_node(
      ss::shared_ptr<stm> s,
      cloud_io::remote* remote,
      const cloud_storage_clients::bucket_name& bucket,
      const ss::sstring& staging_path)
      : stm_ptr(std::move(s))
      , remote(remote)
      , bucket(bucket)
      , staging_directory(staging_path.data()) {}

    ss::future<std::expected<replicated_database*, replicated_database::errc>>
    open_db() {
        auto ret = co_await replicated_database::open(
          stm_ptr.get(), staging_directory.get_path(), remote, bucket, as);
        if (!ret.has_value()) {
            co_return std::unexpected(ret.error());
        }
        auto ptr = ret.value().get();
        dbs.push_back(std::move(ret.value()));
        co_return ptr;
    }

    ss::future<> close() {
        for (auto& db : dbs) {
            auto res = co_await db->close();
            if (!res.has_value()) {
                vlog(
                  rdb_test_log.warn,
                  "Failed to close DB for node {}",
                  stm_ptr->raft()->self());
            }
        }
    }

    ss::shared_ptr<stm> stm_ptr;
    cloud_io::remote* remote;
    const cloud_storage_clients::bucket_name& bucket;
    temporary_dir staging_directory;
    ss::abort_source as;
    std::list<std::unique_ptr<replicated_database>> dbs;
};

} // namespace

class ReplicatedDatabaseTest
  : public raft::raft_fixture
  , public s3_imposter_fixture {
public:
    static constexpr auto num_nodes = 3;
    using opt_ref = std::optional<std::reference_wrapper<replicated_db_node>>;

    void SetUp() override {
        ss::smp::invoke_on_all([] {
            config::node().node_id.set_value(model::node_id{1});
        }).get();
        cfg.get("raft_heartbeat_interval_ms").set_value(50ms);
        cfg.get("raft_heartbeat_timeout_ms").set_value(500ms);

        // Set up S3 imposter
        set_expectations_and_listen({});

        // Set up remote
        sr = cloud_io::scoped_remote::create(10, conf);

        raft::raft_fixture::SetUpAsync().get();

        // Create raft nodes with STMs
        for (auto i = 0; i < num_nodes; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            node->initialise(all_vnodes()).get();
            auto* raft = node->raft().get();
            raft::state_machine_manager_builder builder;

            // Create STM for this node
            auto s = builder.create_stm<stm>(
              rdb_test_log,
              raft,
              config::mock_binding<std::chrono::seconds>(1s));

            node->start(std::move(builder)).get();

            // Create staging directory for this node
            auto staging_path = fmt::format("replicated_db_test_{}", id());
            db_nodes.at(id()) = std::make_unique<replicated_db_node>(
              std::move(s), &sr->remote.local(), bucket_name, staging_path);
        }

        // Wait for leader election
        opt_ref leader;
        wait_for_leader(leader).get();
    }

    void TearDown() override {
        // Clean up staging directories
        for (auto& node : db_nodes) {
            if (node) {
                node->close().get();
            }
        }

        raft::raft_fixture::TearDownAsync().get();
        sr.reset();
    }

    // Returns the node on the current leader
    opt_ref leader_node() {
        auto leader_id = get_leader();
        if (!leader_id.has_value()) {
            return std::nullopt;
        }
        auto& node = *db_nodes.at(leader_id.value()());
        if (!node.stm_ptr->raft()->is_leader()) {
            return std::nullopt;
        }
        return node;
    }

    // Waits for a stable leader to be elected, and returns it
    ss::future<> wait_for_leader(opt_ref& leader) {
        RPTEST_REQUIRE_EVENTUALLY_CORO(10s, [&] {
            leader = leader_node();
            return leader.has_value();
        });
    }

    // Waits for all nodes to have applied the current committed offset
    ss::future<> wait_for_apply() {
        model::offset committed_offset{};
        for (auto& n : nodes()) {
            committed_offset = std::max(
              committed_offset, n.second->raft()->committed_offset());
        }

        co_await parallel_for_each_node([committed_offset](auto& node) {
            return node.raft()->stm_manager()->wait(
              committed_offset, model::no_timeout);
        });
    }

    std::array<std::unique_ptr<replicated_db_node>, num_nodes> db_nodes;
    scoped_config cfg;
    std::unique_ptr<cloud_io::scoped_remote> sr;
};

TEST_F(ReplicatedDatabaseTest, TestWriteAfterLeadershipChange) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write some initial data
    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });

    auto write_result1 = db.write(std::move(rows1)).get();
    ASSERT_TRUE(write_result1.has_value());
    wait_for_apply().get();

    // Step down the leader
    leader.stm_ptr->raft()->step_down("test").get();

    // Wait for new leader
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& new_leader = leader_opt->get();

    // Open database on new leader
    auto new_db_result = new_leader.open_db().get();
    ASSERT_TRUE(new_db_result.has_value());
    auto& new_db = *new_db_result.value();
    chunked_vector<write_batch_row> rows2;
    rows2.emplace_back(
      write_batch_row{
        .key = "key2",
        .value = iobuf::from("value2"),
      });

    auto write_result2 = new_db.write(std::move(rows2)).get();
    ASSERT_TRUE(write_result2.has_value());
    auto it = new_db.db().create_iterator().get();
    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(new_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 2);

    wait_for_apply().get();
    for (const auto& node : db_nodes) {
        ASSERT_GE(node->stm_ptr->state().volatile_buffer.size(), 2);
    }
}

TEST_F(ReplicatedDatabaseTest, TestFlush) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write some initial data
    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key1",
        .value = iobuf::from("value1"),
      });

    auto write_result1 = db.write(std::move(rows1)).get();
    ASSERT_TRUE(write_result1.has_value());
    ASSERT_EQ(1, leader.stm_ptr->state().volatile_buffer.size());

    auto flush_res = db.flush().get();
    ASSERT_TRUE(flush_res.has_value());
    ASSERT_EQ(0, leader.stm_ptr->state().volatile_buffer.size());

    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 1);
}

TEST_F(ReplicatedDatabaseTest, TestResetWithEmptyManifest) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Get the initial domain UUID
    auto initial_uuid = db.get_domain_uuid();
    ASSERT_FALSE(initial_uuid().is_nil());

    // Create a new UUID and reset
    auto new_uuid = domain_uuid(uuid_t::create());
    ASSERT_NE(initial_uuid, new_uuid);

    auto reset_result = db.reset(new_uuid, std::nullopt).get();
    ASSERT_TRUE(reset_result.has_value());

    // Verify the domain UUID was updated
    ASSERT_EQ(db.get_domain_uuid(), new_uuid);
    ASSERT_EQ(leader.stm_ptr->state().domain_uuid, new_uuid);

    // Verify database is still functional - write and read data
    chunked_vector<write_batch_row> rows;
    rows.emplace_back(
      write_batch_row{
        .key = "key_after_reset",
        .value = iobuf::from("value_after_reset"),
      });

    auto write_result = db.write(std::move(rows)).get();
    ASSERT_TRUE(write_result.has_value());

    // Verify data is readable
    auto it = db.db().create_iterator().get();
    it.seek_to_first().get();
    ASSERT_TRUE(it.valid());
}

TEST_F(ReplicatedDatabaseTest, TestReset) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Get the initial domain UUID
    auto initial_uuid = db.get_domain_uuid();
    ASSERT_FALSE(initial_uuid().is_nil());

    // Create a new UUID and reset
    auto new_uuid = domain_uuid(uuid_t::create());
    auto domain_prefix = cloud_storage_clients::object_key{
      fmt::format("{}", new_uuid)};
    ASSERT_NE(initial_uuid, new_uuid);

    // Persist a manifest that has some data in it.
    temporary_dir tmp("tmpdata");
    auto cloud_db
      = lsm::database::open(
          {.database_epoch = 0},
          lsm::io::persistence{
            .data
            = lsm::io::open_cloud_data_persistence(
                tmp.get_path(), &sr->remote.local(), bucket_name, domain_prefix)
                .get(),
            .metadata = lsm::io::open_cloud_metadata_persistence(
                          &sr->remote.local(), bucket_name, domain_prefix)
                          .get(),
          })
          .get();

    auto wb = cloud_db.create_write_batch();
    wb.put(
      "key_before_reset",
      iobuf::from("value_before_reset"),
      lsm::sequence_number{10});
    cloud_db.apply(std::move(wb)).get();
    cloud_db.flush().get();
    cloud_db.close().get();
    auto cloud_meta_persistence = lsm::io::open_cloud_metadata_persistence(
                                    &sr->remote.local(),
                                    bucket_name,
                                    domain_prefix)
                                    .get();
    auto cloud_buf = cloud_meta_persistence
                       ->read_manifest(lsm::internal::database_epoch::max())
                       .get();
    std::optional<lsm::proto::manifest> manifest;
    if (cloud_buf) {
        manifest
          = lsm::proto::manifest::from_proto(std::move(*cloud_buf)).get();
    }

    auto reset_result = db.reset(new_uuid, std::move(manifest)).get();
    ASSERT_TRUE(reset_result.has_value());

    // Verify the domain UUID was updated
    ASSERT_EQ(db.get_domain_uuid(), new_uuid);
    ASSERT_EQ(leader.stm_ptr->state().domain_uuid, new_uuid);

    // Verify data is readable
    size_t rows_count = 0;
    db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& reopened_db = *db_result.value();
    ASSERT_NO_THROW(count_rows(reopened_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 1);
    ASSERT_GT(leader.stm_ptr->state().seqno_delta, 0);
    ASSERT_EQ(0, leader.stm_ptr->state().volatile_buffer.size());

    chunked_vector<write_batch_row> rows1;
    rows1.emplace_back(
      write_batch_row{
        .key = "key_after_reset",
        .value = iobuf::from("value_after_reset"),
      });

    auto write_result = reopened_db.write(std::move(rows1)).get();
    ASSERT_TRUE(write_result.has_value());
    ASSERT_EQ(1, leader.stm_ptr->state().volatile_buffer.size());

    ASSERT_NO_THROW(count_rows(reopened_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 2);
}

TEST_F(ReplicatedDatabaseTest, TestWriteLifecycleAndFlushBehavior) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write 5 batches sequentially without flushing
    for (int i = 0; i < 5; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(write_batch_row{
          .key = fmt::format("key{}", i),
          .value = iobuf::from(fmt::format("value{}", i)),
        });
        auto write_result = db.write(std::move(rows)).get();
        ASSERT_TRUE(write_result.has_value());
    }

    // Verify volatile_buffer size grows to match number of batches
    ASSERT_EQ(5, leader.stm_ptr->state().volatile_buffer.size());

    // Verify all data is readable
    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 5);

    // Test overwriting a key (write key0 again with different value)
    chunked_vector<write_batch_row> overwrite_rows;
    overwrite_rows.emplace_back(write_batch_row{
      .key = "key0",
      .value = iobuf::from("updated_value0"),
    });
    auto overwrite_result = db.write(std::move(overwrite_rows)).get();
    ASSERT_TRUE(overwrite_result.has_value());
    ASSERT_EQ(6, leader.stm_ptr->state().volatile_buffer.size());

    // Verify still 5 rows (key overwritten, not duplicated)
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 5);

    // Verify the key has the updated value
    auto it = db.db().create_iterator().get();
    it.seek("key0").get();
    ASSERT_TRUE(it.valid());
    ASSERT_EQ(it.key(), "key0");
    auto value_buf = it.value();
    ASSERT_EQ(value_buf.size_bytes(), strlen("updated_value0"));

    // Flush
    auto flush_result = db.flush().get();
    ASSERT_TRUE(flush_result.has_value());

    // Verify volatile_buffer is now empty
    ASSERT_EQ(0, leader.stm_ptr->state().volatile_buffer.size());

    // Verify all data is still readable
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 5);

    // Write more data after flush
    chunked_vector<write_batch_row> post_flush_rows;
    post_flush_rows.emplace_back(write_batch_row{
      .key = "key_after_flush",
      .value = iobuf::from("value_after_flush"),
    });
    auto post_flush_result = db.write(std::move(post_flush_rows)).get();
    ASSERT_TRUE(post_flush_result.has_value());

    // Verify new write appears
    ASSERT_EQ(1, leader.stm_ptr->state().volatile_buffer.size());
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 6);
}

TEST_F(ReplicatedDatabaseTest, TestVolatileBufferRecoveryOnLeadershipChange) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write 3 batches on leader, flush
    for (int i = 0; i < 3; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(write_batch_row{
          .key = fmt::format("flushed_key{}", i),
          .value = iobuf::from(fmt::format("flushed_value{}", i)),
        });
        auto write_result = db.write(std::move(rows)).get();
        ASSERT_TRUE(write_result.has_value());
    }
    auto flush_result = db.flush().get();
    ASSERT_TRUE(flush_result.has_value());
    ASSERT_EQ(0, leader.stm_ptr->state().volatile_buffer.size());

    // Write 2 more batches without flushing
    for (int i = 0; i < 2; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(write_batch_row{
          .key = fmt::format("volatile_key{}", i),
          .value = iobuf::from(fmt::format("volatile_value{}", i)),
        });
        auto write_result = db.write(std::move(rows)).get();
        ASSERT_TRUE(write_result.has_value());
    }

    // Verify volatile_buffer has 2 entries
    ASSERT_EQ(2, leader.stm_ptr->state().volatile_buffer.size());
    wait_for_apply().get();

    // Step down leader
    leader.stm_ptr->raft()->step_down("test").get();

    // Wait for new leader election
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& new_leader = leader_opt->get();

    // Open DB on new leader
    auto new_db_result = new_leader.open_db().get();
    ASSERT_TRUE(new_db_result.has_value());
    auto& new_db = *new_db_result.value();

    // Verify the new leader's DB contains all 5 batches (both flushed and volatile)
    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(new_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 5);

    // Write another batch on new leader
    chunked_vector<write_batch_row> new_leader_rows;
    new_leader_rows.emplace_back(write_batch_row{
      .key = "new_leader_key",
      .value = iobuf::from("new_leader_value"),
    });
    auto new_leader_write = new_db.write(std::move(new_leader_rows)).get();
    ASSERT_TRUE(new_leader_write.has_value());

    // Verify all 6 batches are readable
    ASSERT_NO_THROW(count_rows(new_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 6);
}

TEST_F(ReplicatedDatabaseTest, TestClusterWideStateConsistency) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write several batches on leader
    for (int i = 0; i < 4; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(write_batch_row{
          .key = fmt::format("cluster_key{}", i),
          .value = iobuf::from(fmt::format("cluster_value{}", i)),
        });
        auto write_result = db.write(std::move(rows)).get();
        ASSERT_TRUE(write_result.has_value());
    }

    // Wait for apply across all nodes
    wait_for_apply().get();

    // Verify all nodes have identical volatile_buffer sizes
    auto expected_size = leader.stm_ptr->state().volatile_buffer.size();
    ASSERT_EQ(expected_size, 4);
    for (const auto& node : db_nodes) {
        ASSERT_EQ(node->stm_ptr->state().volatile_buffer.size(), expected_size);
    }

    // Verify volatile_buffer contents match across nodes
    auto& leader_buffer = leader.stm_ptr->state().volatile_buffer;
    for (const auto& node : db_nodes) {
        auto& node_buffer = node->stm_ptr->state().volatile_buffer;
        ASSERT_EQ(node_buffer.size(), leader_buffer.size());
        for (size_t i = 0; i < leader_buffer.size(); ++i) {
            // Buffers should contain the same writes in the same order
            ASSERT_EQ(node_buffer[i].size(), leader_buffer[i].size());
        }
    }

    // Verify domain_uuid is consistent across all nodes
    auto expected_uuid = leader.stm_ptr->state().domain_uuid;
    for (const auto& node : db_nodes) {
        ASSERT_EQ(node->stm_ptr->state().domain_uuid, expected_uuid);
    }

    // Flush on leader
    auto flush_result = db.flush().get();
    ASSERT_TRUE(flush_result.has_value());

    // Wait for apply
    wait_for_apply().get();

    // Verify all nodes' volatile_buffers are empty
    for (const auto& node : db_nodes) {
        ASSERT_EQ(node->stm_ptr->state().volatile_buffer.size(), 0);
    }
}

TEST_F(ReplicatedDatabaseTest, TestDataPersistenceAndReopen) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Write data and flush
    chunked_vector<write_batch_row> initial_rows;
    initial_rows.emplace_back(write_batch_row{
      .key = "persistent_key1",
      .value = iobuf::from("persistent_value1"),
    });
    initial_rows.emplace_back(write_batch_row{
      .key = "persistent_key2",
      .value = iobuf::from("persistent_value2"),
    });
    auto write_result = db.write(std::move(initial_rows)).get();
    ASSERT_TRUE(write_result.has_value());

    auto flush_result = db.flush().get();
    ASSERT_TRUE(flush_result.has_value());

    // Record the domain_uuid before close
    auto original_uuid = db.get_domain_uuid();

    // Close DB
    auto close_result = db.close().get();
    ASSERT_TRUE(close_result.has_value());

    // Reopen DB on same leader (no leadership change)
    auto reopen_result = leader.open_db().get();
    ASSERT_TRUE(reopen_result.has_value());
    auto& reopened_db = *reopen_result.value();

    // Verify flushed data is readable after reopen
    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(reopened_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 2);

    // Verify domain_uuid is unchanged
    ASSERT_EQ(reopened_db.get_domain_uuid(), original_uuid);

    // Write new data after reopen
    chunked_vector<write_batch_row> new_rows;
    new_rows.emplace_back(write_batch_row{
      .key = "persistent_key3",
      .value = iobuf::from("persistent_value3"),
    });
    auto new_write_result = reopened_db.write(std::move(new_rows)).get();
    ASSERT_TRUE(new_write_result.has_value());

    // Flush again
    auto second_flush = reopened_db.flush().get();
    ASSERT_TRUE(second_flush.has_value());

    // Close and reopen again
    auto second_close = reopened_db.close().get();
    ASSERT_TRUE(second_close.has_value());

    auto third_open = leader.open_db().get();
    ASSERT_TRUE(third_open.has_value());
    auto& final_db = *third_open.value();

    // Verify both old and new data persist
    ASSERT_NO_THROW(count_rows(final_db.db(), rows_count).get());
    ASSERT_EQ(rows_count, 3);

    // Verify all keys are present
    auto it = final_db.db().create_iterator().get();
    it.seek_to_first().get();
    int found_keys = 0;
    while (it.valid()) {
        auto key = it.key();
        ASSERT_TRUE(
          key == "persistent_key1" || key == "persistent_key2"
          || key == "persistent_key3");
        ++found_keys;
        it.next().get();
    }
    ASSERT_EQ(found_keys, 3);
}

TEST_F(ReplicatedDatabaseTest, TestConcurrentWrites) {
    opt_ref leader_opt;
    ASSERT_NO_FATAL_FAILURE(wait_for_leader(leader_opt).get());
    auto& leader = leader_opt->get();

    // Open the database on the leader
    auto db_result = leader.open_db().get();
    ASSERT_TRUE(db_result.has_value());
    auto& db = *db_result.value();

    // Launch 10 concurrent write operations with different keys
    constexpr int num_concurrent_writes = 10;
    std::vector<ss::future<std::expected<void, replicated_database::errc>>>
      write_futures;
    write_futures.reserve(num_concurrent_writes);

    for (int i = 0; i < num_concurrent_writes; ++i) {
        chunked_vector<write_batch_row> rows;
        rows.emplace_back(write_batch_row{
          .key = fmt::format("concurrent_key{}", i),
          .value = iobuf::from(fmt::format("concurrent_value{}", i)),
        });
        write_futures.push_back(db.write(std::move(rows)));
    }

    // Wait for all writes to complete
    auto results = ss::when_all_succeed(
                     write_futures.begin(), write_futures.end())
                     .get();

    // Verify all writes succeeded
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
    }

    // Verify volatile_buffer has all writes
    ASSERT_EQ(
      num_concurrent_writes, leader.stm_ptr->state().volatile_buffer.size());

    // Verify all data is readable
    size_t rows_count = 0;
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, num_concurrent_writes);

    // Verify each key is present with correct value
    for (int i = 0; i < num_concurrent_writes; ++i) {
        auto it = db.db().create_iterator().get();
        it.seek(fmt::format("concurrent_key{}", i)).get();
        ASSERT_TRUE(it.valid());
        ASSERT_EQ(it.key(), fmt::format("concurrent_key{}", i));

        auto value_buf = it.value();
        auto expected_value = fmt::format("concurrent_value{}", i);
        ASSERT_EQ(value_buf.size_bytes(), expected_value.size());
    }

    // Wait for replication and verify cluster consistency
    wait_for_apply().get();
    for (const auto& node : db_nodes) {
        ASSERT_EQ(
          node->stm_ptr->state().volatile_buffer.size(),
          num_concurrent_writes);
    }

    // Flush and verify all data persists
    auto flush_result = db.flush().get();
    ASSERT_TRUE(flush_result.has_value());
    ASSERT_EQ(0, leader.stm_ptr->state().volatile_buffer.size());

    // Verify data is still readable after flush
    ASSERT_NO_THROW(count_rows(db.db(), rows_count).get());
    ASSERT_EQ(rows_count, num_concurrent_writes);
}
