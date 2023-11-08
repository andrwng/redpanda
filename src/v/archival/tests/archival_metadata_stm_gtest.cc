// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cloud_storage_clients/client_pool.h"
#include "cluster/archival_metadata_stm.h"
#include "raft/tests/raft_fixture.h"
#include "test_utils/test.h"

#include <seastar/coroutine/parallel_for_each.hh>

using cloud_storage::segment_name;
using segment_meta = cloud_storage::partition_manifest::segment_meta;

namespace {
ss::logger fixture_logger{"archival_stm_fixture"};

constexpr const char* httpd_host_name = "127.0.0.1";
constexpr uint16_t httpd_port_number = 4442;

cloud_storage_clients::s3_configuration get_configuration() {
    net::unresolved_address server_addr(httpd_host_name, httpd_port_number);
    cloud_storage_clients::s3_configuration conf;
    conf.uri = cloud_storage_clients::access_point_uri(httpd_host_name);
    conf.access_key = cloud_roles::public_key_str("acess-key");
    conf.secret_key = cloud_roles::private_key_str("secret-key");
    conf.region = cloud_roles::aws_region_name("us-east-1");
    conf.server_addr = server_addr;
    conf.disable_metrics = net::metrics_disabled::yes;
    conf.disable_public_metrics = net::public_metrics_disabled::yes;
    conf._probe = ss::make_shared<cloud_storage_clients::client_probe>(
      net::metrics_disabled::yes,
      net::public_metrics_disabled::yes,
      cloud_roles::aws_region_name{"us-east-1"},
      cloud_storage_clients::endpoint_url{httpd_host_name});
    return conf;
}

constexpr model::cloud_credentials_source config_file{
  model::cloud_credentials_source::config_file};
} // namespace

struct archival_stm_node {
    archival_stm_node() = default;

    ss::shared_ptr<cluster::archival_metadata_stm> archival_stm;
    ss::sharded<cloud_storage_clients::client_pool> client_pool;
    ss::sharded<cloud_storage::remote> remote;
};

class archival_metadata_stm_gtest_fixture : public raft::raft_fixture {
public:
    static constexpr auto node_count = 3;

    seastar::future<> TearDownAsync() override {
        co_await seastar::coroutine::parallel_for_each(
          _archival_stm_nodes, [](archival_stm_node& node) {
              return node.remote.stop().then(
                [&node]() { return node.client_pool.stop(); });
          });

        co_await raft::raft_fixture::TearDownAsync();
    }

    ss::future<> start() {
        for (auto i = 0; i < node_count; ++i) {
            add_node(model::node_id(i), model::revision_id(0));
        }

        for (auto& [id, node] : nodes()) {
            auto& stm_node = _archival_stm_nodes.at(id());

            co_await stm_node.client_pool.start(
              10, ss::sharded_parameter([]() { return get_configuration(); }));

            co_await stm_node.remote.start(
              std::ref(stm_node.client_pool),
              ss::sharded_parameter([] { return get_configuration(); }),
              ss::sharded_parameter([] { return config_file; }));

            co_await node->initialise(all_vnodes());

            raft::state_machine_manager_builder builder;
            auto stm = builder.create_stm<cluster::archival_metadata_stm>(
              node->raft().get(),
              stm_node.remote.local(),
              node->get_feature_table().local(),
              fixture_logger);

            stm_node.archival_stm = std::move(stm);

            vlog(fixture_logger.info, "Starting node {}", id);

            co_await node->start(std::move(builder));
        }
    }

    cluster::archival_metadata_stm& get_leader_stm() {
        const auto leader = get_leader();
        if (!leader) {
            throw std::runtime_error{"No leader"};
        }

        auto ptr = _archival_stm_nodes.at(*leader).archival_stm;
        if (!ptr) {
            throw std::runtime_error{
              ssx::sformat("Achival stm for node {} not initialised", *leader)};
        }

        return *ptr;
    }

private:
    std::array<archival_stm_node, node_count> _archival_stm_nodes;
};

TEST_F_CORO(archival_metadata_stm_gtest_fixture, test_archival_stm_happy_path) {
    ss::abort_source never_abort;

    co_await start();

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await wait_for_leader(10s);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    co_await get_leader_stm().add_segments(
      m,
      std::nullopt,
      ss::lowres_clock::now() + 10s,
      never_abort,
      cluster::segment_validated::yes);

    ASSERT_EQ_CORO(get_leader_stm().manifest().size(), 1);
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->base_offset, model::offset(0));
    ASSERT_EQ_CORO(
      get_leader_stm().manifest().begin()->committed_offset, model::offset(99));

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::dirty);

    co_await get_leader_stm().mark_clean(
      ss::lowres_clock::now() + 10s,
      get_leader_stm().get_insync_offset(),
      never_abort);

    ASSERT_EQ_CORO(
      get_leader_stm().get_dirty(),
      cluster::archival_metadata_stm::state_dirty::clean);
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture,
  test_same_term_sync_pending_replication_success) {
    /*
     * Test that archival_metadata_stm::sync is able to sync
     * within the same term and that it will wait for on-going
     * replication futures to complete before doing so. To simulate
     * this scenario we introduce a small delay on append entries
     * response processing.
     */

    ss::abort_source never_abort;

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await start();

    auto res = co_await with_leader(
      10s, [this, &m, &never_abort](raft::raft_node_instance&) {
          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    ASSERT_TRUE_CORO(!res);

    auto [plagued_node, delay_applied] = co_await with_leader(
      10s, [](raft::raft_node_instance& node) {
          raft::response_delay fail{
            .length = 100ms, .on_applied = ss::promise<>{}};

          auto delay_applied = fail.on_applied->get_future();
          node.inject_failure(raft::msg_type::append_entries, std::move(fail));
          return std::make_tuple(node.get_vnode(), std::move(delay_applied));
      });

    m.clear();
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(1)});

    auto slow_replication_fut = with_leader(
      10s,
      [this, &m, &never_abort, &plagued_node](raft::raft_node_instance& node) {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    co_await std::move(delay_applied);

    auto synced = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().sync(10s);
      });

    ASSERT_TRUE_CORO(synced);

    auto slow_replication_res = co_await std::move(slow_replication_fut);
    ASSERT_TRUE_CORO(!slow_replication_res);

    auto [committed_offset, term] = co_await with_leader(
      10s, [](raft::raft_node_instance& node) mutable {
          return std::make_tuple(
            node.raft()->committed_offset(), node.raft()->term());
      });

    ASSERT_EQ_CORO(committed_offset, model::offset{2});
    ASSERT_EQ_CORO(term, model::term_id{1});
}

TEST_F_CORO(
    archival_metadata_stm_gtest_fixture,
    test_sequence) {
    std::vector<cloud_storage::segment_meta> metas = {
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1047,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(0),
        .base_timestamp = model::timestamp(1672995602001),
        .max_timestamp = model::timestamp(1672995602001),
        .delta_offset = model::offset_delta(0),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(2),
        .segment_term = model::term_id(1),
        .delta_offset_end = model::offset_delta(0),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1237,
        .base_offset = model::offset(1),
        .committed_offset = model::offset(2),
        .base_timestamp = model::timestamp(1672995688686),
        .max_timestamp = model::timestamp(1672995688686),
        .delta_offset = model::offset_delta(1),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(3),
        .segment_term = model::term_id(2),
        .delta_offset_end = model::offset_delta(2),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2284,
        .base_offset = model::offset(0),
        .committed_offset = model::offset(2),
        .base_timestamp = model::timestamp(1672995602001),
        .max_timestamp = model::timestamp(1672995688943),
        .delta_offset = model::offset_delta(0),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(3),
        .segment_term = model::term_id(1),
        .delta_offset_end = model::offset_delta(2),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1427,
        .base_offset = model::offset(3),
        .committed_offset = model::offset(5),
        .base_timestamp = model::timestamp(1674042282756),
        .max_timestamp = model::timestamp(1674042282756),
        .delta_offset = model::offset_delta(3),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(4),
        .segment_term = model::term_id(3),
        .delta_offset_end = model::offset_delta(5),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1427,
        .base_offset = model::offset(3),
        .committed_offset = model::offset(5),
        .base_timestamp = model::timestamp(1674042282756),
        .max_timestamp = model::timestamp(1674042295966),
        .delta_offset = model::offset_delta(3),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(4),
        .segment_term = model::term_id(3),
        .delta_offset_end = model::offset_delta(5),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1496,
        .base_offset = model::offset(6),
        .committed_offset = model::offset(9),
        .base_timestamp = model::timestamp(1674042321750),
        .max_timestamp = model::timestamp(1674042321750),
        .delta_offset = model::offset_delta(6),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(5),
        .segment_term = model::term_id(4),
        .delta_offset_end = model::offset_delta(9),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1496,
        .base_offset = model::offset(6),
        .committed_offset = model::offset(9),
        .base_timestamp = model::timestamp(1674042321750),
        .max_timestamp = model::timestamp(1674042626557),
        .delta_offset = model::offset_delta(6),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(5),
        .segment_term = model::term_id(4),
        .delta_offset_end = model::offset_delta(9),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1427,
        .base_offset = model::offset(10),
        .committed_offset = model::offset(12),
        .base_timestamp = model::timestamp(1674731426463),
        .max_timestamp = model::timestamp(1674731426463),
        .delta_offset = model::offset_delta(10),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(6),
        .segment_term = model::term_id(5),
        .delta_offset_end = model::offset_delta(12),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1427,
        .base_offset = model::offset(10),
        .committed_offset = model::offset(12),
        .base_timestamp = model::timestamp(1674731426463),
        .max_timestamp = model::timestamp(1674731433825),
        .delta_offset = model::offset_delta(10),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(6),
        .segment_term = model::term_id(5),
        .delta_offset_end = model::offset_delta(12),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1429,
        .base_offset = model::offset(13),
        .committed_offset = model::offset(15),
        .base_timestamp = model::timestamp(1674731463061),
        .max_timestamp = model::timestamp(1674731463061),
        .delta_offset = model::offset_delta(13),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(7),
        .segment_term = model::term_id(6),
        .delta_offset_end = model::offset_delta(15),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1429,
        .base_offset = model::offset(13),
        .committed_offset = model::offset(15),
        .base_timestamp = model::timestamp(1674731463061),
        .max_timestamp = model::timestamp(1674731466972),
        .delta_offset = model::offset_delta(13),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(7),
        .segment_term = model::term_id(6),
        .delta_offset_end = model::offset_delta(15),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1429,
        .base_offset = model::offset(16),
        .committed_offset = model::offset(18),
        .base_timestamp = model::timestamp(1674731493089),
        .max_timestamp = model::timestamp(1674731493089),
        .delta_offset = model::offset_delta(16),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(8),
        .segment_term = model::term_id(7),
        .delta_offset_end = model::offset_delta(18),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1429,
        .base_offset = model::offset(16),
        .committed_offset = model::offset(18),
        .base_timestamp = model::timestamp(1674731493089),
        .max_timestamp = model::timestamp(1674731507088),
        .delta_offset = model::offset_delta(16),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(8),
        .segment_term = model::term_id(7),
        .delta_offset_end = model::offset_delta(18),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1498,
        .base_offset = model::offset(19),
        .committed_offset = model::offset(22),
        .base_timestamp = model::timestamp(1674731525793),
        .max_timestamp = model::timestamp(1674731525793),
        .delta_offset = model::offset_delta(19),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(9),
        .segment_term = model::term_id(8),
        .delta_offset_end = model::offset_delta(22),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1498,
        .base_offset = model::offset(19),
        .committed_offset = model::offset(22),
        .base_timestamp = model::timestamp(1674731525793),
        .max_timestamp = model::timestamp(1674731830138),
        .delta_offset = model::offset_delta(19),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(9),
        .segment_term = model::term_id(8),
        .delta_offset_end = model::offset_delta(22),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1429,
        .base_offset = model::offset(23),
        .committed_offset = model::offset(25),
        .base_timestamp = model::timestamp(1675776958868),
        .max_timestamp = model::timestamp(1675776958868),
        .delta_offset = model::offset_delta(23),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(10),
        .segment_term = model::term_id(9),
        .delta_offset_end = model::offset_delta(25),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1429,
        .base_offset = model::offset(23),
        .committed_offset = model::offset(25),
        .base_timestamp = model::timestamp(1675776958868),
        .max_timestamp = model::timestamp(1675776966461),
        .delta_offset = model::offset_delta(23),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(10),
        .segment_term = model::term_id(9),
        .delta_offset_end = model::offset_delta(25),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1429,
        .base_offset = model::offset(26),
        .committed_offset = model::offset(28),
        .base_timestamp = model::timestamp(1675776990868),
        .max_timestamp = model::timestamp(1675776990868),
        .delta_offset = model::offset_delta(26),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(11),
        .segment_term = model::term_id(10),
        .delta_offset_end = model::offset_delta(28),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1429,
        .base_offset = model::offset(26),
        .committed_offset = model::offset(28),
        .base_timestamp = model::timestamp(1675776990868),
        .max_timestamp = model::timestamp(1675777004053),
        .delta_offset = model::offset_delta(26),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(11),
        .segment_term = model::term_id(10),
        .delta_offset_end = model::offset_delta(28),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(29),
        .committed_offset = model::offset(32),
        .base_timestamp = model::timestamp(1675777020880),
        .max_timestamp = model::timestamp(1675777020880),
        .delta_offset = model::offset_delta(29),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(12),
        .segment_term = model::term_id(11),
        .delta_offset_end = model::offset_delta(32),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1500,
        .base_offset = model::offset(29),
        .committed_offset = model::offset(32),
        .base_timestamp = model::timestamp(1675777020880),
        .max_timestamp = model::timestamp(1675777325627),
        .delta_offset = model::offset_delta(29),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(12),
        .segment_term = model::term_id(11),
        .delta_offset_end = model::offset_delta(32),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1431,
        .base_offset = model::offset(33),
        .committed_offset = model::offset(35),
        .base_timestamp = model::timestamp(1676909531052),
        .max_timestamp = model::timestamp(1676909531052),
        .delta_offset = model::offset_delta(33),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(13),
        .segment_term = model::term_id(12),
        .delta_offset_end = model::offset_delta(35),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1431,
        .base_offset = model::offset(33),
        .committed_offset = model::offset(35),
        .base_timestamp = model::timestamp(1676909531052),
        .max_timestamp = model::timestamp(1676909534956),
        .delta_offset = model::offset_delta(33),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(13),
        .segment_term = model::term_id(12),
        .delta_offset_end = model::offset_delta(35),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(36),
        .committed_offset = model::offset(37),
        .base_timestamp = model::timestamp(1676909568140),
        .max_timestamp = model::timestamp(1676909568140),
        .delta_offset = model::offset_delta(36),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(14),
        .segment_term = model::term_id(13),
        .delta_offset_end = model::offset_delta(37),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 192,
        .base_offset = model::offset(38),
        .committed_offset = model::offset(38),
        .base_timestamp = model::timestamp(1676909581772),
        .max_timestamp = model::timestamp(1676909581772),
        .delta_offset = model::offset_delta(38),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(14),
        .segment_term = model::term_id(13),
        .delta_offset_end = model::offset_delta(38),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1239,
        .base_offset = model::offset(36),
        .committed_offset = model::offset(37),
        .base_timestamp = model::timestamp(1676909568140),
        .max_timestamp = model::timestamp(1676909568785),
        .delta_offset = model::offset_delta(36),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(14),
        .segment_term = model::term_id(13),
        .delta_offset_end = model::offset_delta(37),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1631,
        .base_offset = model::offset(39),
        .committed_offset = model::offset(43),
        .base_timestamp = model::timestamp(1676909626244),
        .max_timestamp = model::timestamp(1676909626244),
        .delta_offset = model::offset_delta(39),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(15),
        .segment_term = model::term_id(14),
        .delta_offset_end = model::offset_delta(43),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1431,
        .base_offset = model::offset(36),
        .committed_offset = model::offset(38),
        .base_timestamp = model::timestamp(1676909568140),
        .max_timestamp = model::timestamp(1676909581772),
        .delta_offset = model::offset_delta(36),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(15),
        .segment_term = model::term_id(13),
        .delta_offset_end = model::offset_delta(38),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1631,
        .base_offset = model::offset(39),
        .committed_offset = model::offset(43),
        .base_timestamp = model::timestamp(1676909626244),
        .max_timestamp = model::timestamp(1676909930930),
        .delta_offset = model::offset_delta(39),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(15),
        .segment_term = model::term_id(14),
        .delta_offset_end = model::offset_delta(43),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1692,
        .base_offset = model::offset(44),
        .committed_offset = model::offset(48),
        .base_timestamp = model::timestamp(1677690095764),
        .max_timestamp = model::timestamp(1677690095764),
        .delta_offset = model::offset_delta(44),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(16),
        .segment_term = model::term_id(15),
        .delta_offset_end = model::offset_delta(48),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1692,
        .base_offset = model::offset(44),
        .committed_offset = model::offset(48),
        .base_timestamp = model::timestamp(1677690095764),
        .max_timestamp = model::timestamp(1677690399907),
        .delta_offset = model::offset_delta(44),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(16),
        .segment_term = model::term_id(15),
        .delta_offset_end = model::offset_delta(48),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(49),
        .committed_offset = model::offset(52),
        .base_timestamp = model::timestamp(1678831019158),
        .max_timestamp = model::timestamp(1678831019158),
        .delta_offset = model::offset_delta(49),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(17),
        .segment_term = model::term_id(16),
        .delta_offset_end = model::offset_delta(52),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1500,
        .base_offset = model::offset(49),
        .committed_offset = model::offset(52),
        .base_timestamp = model::timestamp(1678831019158),
        .max_timestamp = model::timestamp(1678831323662),
        .delta_offset = model::offset_delta(49),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(17),
        .segment_term = model::term_id(16),
        .delta_offset_end = model::offset_delta(52),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(53),
        .committed_offset = model::offset(56),
        .base_timestamp = model::timestamp(1678849739179),
        .max_timestamp = model::timestamp(1678849739179),
        .delta_offset = model::offset_delta(53),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(18),
        .segment_term = model::term_id(17),
        .delta_offset_end = model::offset_delta(56),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(57),
        .committed_offset = model::offset(58),
        .base_timestamp = model::timestamp(1679659696428),
        .max_timestamp = model::timestamp(1679659696428),
        .delta_offset = model::offset_delta(57),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(19),
        .segment_term = model::term_id(18),
        .delta_offset_end = model::offset_delta(58),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(53),
        .committed_offset = model::offset(58),
        .base_timestamp = model::timestamp(1678849739179),
        .max_timestamp = model::timestamp(1679659697064),
        .delta_offset = model::offset_delta(53),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(19),
        .segment_term = model::term_id(17),
        .delta_offset_end = model::offset_delta(58),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(59),
        .committed_offset = model::offset(62),
        .base_timestamp = model::timestamp(1679659758071),
        .max_timestamp = model::timestamp(1679659758071),
        .delta_offset = model::offset_delta(59),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(20),
        .segment_term = model::term_id(19),
        .delta_offset_end = model::offset_delta(62),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(63),
        .committed_offset = model::offset(64),
        .base_timestamp = model::timestamp(1679671147756),
        .max_timestamp = model::timestamp(1679671147756),
        .delta_offset = model::offset_delta(63),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(21),
        .segment_term = model::term_id(20),
        .delta_offset_end = model::offset_delta(64),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(59),
        .committed_offset = model::offset(64),
        .base_timestamp = model::timestamp(1679659758071),
        .max_timestamp = model::timestamp(1679671148038),
        .delta_offset = model::offset_delta(59),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(21),
        .segment_term = model::term_id(19),
        .delta_offset_end = model::offset_delta(64),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(65),
        .committed_offset = model::offset(68),
        .base_timestamp = model::timestamp(1679671181548),
        .max_timestamp = model::timestamp(1679671181548),
        .delta_offset = model::offset_delta(65),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(22),
        .segment_term = model::term_id(21),
        .delta_offset_end = model::offset_delta(68),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(69),
        .committed_offset = model::offset(70),
        .base_timestamp = model::timestamp(1680875101709),
        .max_timestamp = model::timestamp(1680875101709),
        .delta_offset = model::offset_delta(69),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(23),
        .segment_term = model::term_id(22),
        .delta_offset_end = model::offset_delta(70),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(65),
        .committed_offset = model::offset(70),
        .base_timestamp = model::timestamp(1679671181548),
        .max_timestamp = model::timestamp(1680875101949),
        .delta_offset = model::offset_delta(65),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(23),
        .segment_term = model::term_id(21),
        .delta_offset_end = model::offset_delta(70),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(71),
        .committed_offset = model::offset(74),
        .base_timestamp = model::timestamp(1680875197198),
        .max_timestamp = model::timestamp(1680875197198),
        .delta_offset = model::offset_delta(71),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(24),
        .segment_term = model::term_id(23),
        .delta_offset_end = model::offset_delta(74),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(75),
        .committed_offset = model::offset(76),
        .base_timestamp = model::timestamp(1681532726506),
        .max_timestamp = model::timestamp(1681532726506),
        .delta_offset = model::offset_delta(75),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(25),
        .segment_term = model::term_id(24),
        .delta_offset_end = model::offset_delta(76),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(71),
        .committed_offset = model::offset(76),
        .base_timestamp = model::timestamp(1680875197198),
        .max_timestamp = model::timestamp(1681532726626),
        .delta_offset = model::offset_delta(71),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(25),
        .segment_term = model::term_id(23),
        .delta_offset_end = model::offset_delta(76),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1431,
        .base_offset = model::offset(77),
        .committed_offset = model::offset(79),
        .base_timestamp = model::timestamp(1681991230503),
        .max_timestamp = model::timestamp(1681991230503),
        .delta_offset = model::offset_delta(77),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(26),
        .segment_term = model::term_id(25),
        .delta_offset_end = model::offset_delta(79),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1431,
        .base_offset = model::offset(77),
        .committed_offset = model::offset(79),
        .base_timestamp = model::timestamp(1681991230503),
        .max_timestamp = model::timestamp(1681991243835),
        .delta_offset = model::offset_delta(77),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(27),
        .segment_term = model::term_id(25),
        .delta_offset_end = model::offset_delta(80),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1308,
        .base_offset = model::offset(80),
        .committed_offset = model::offset(82),
        .base_timestamp = model::timestamp(1681991260706),
        .max_timestamp = model::timestamp(1681991260706),
        .delta_offset = model::offset_delta(80),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(27),
        .segment_term = model::term_id(26),
        .delta_offset_end = model::offset_delta(83),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1431,
        .base_offset = model::offset(83),
        .committed_offset = model::offset(85),
        .base_timestamp = model::timestamp(1681991327434),
        .max_timestamp = model::timestamp(1681991327434),
        .delta_offset = model::offset_delta(83),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(28),
        .segment_term = model::term_id(27),
        .delta_offset_end = model::offset_delta(86),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(80),
        .committed_offset = model::offset(85),
        .base_timestamp = model::timestamp(1681991260706),
        .max_timestamp = model::timestamp(1681991327689),
        .delta_offset = model::offset_delta(80),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(28),
        .segment_term = model::term_id(26),
        .delta_offset_end = model::offset_delta(86),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(86),
        .committed_offset = model::offset(89),
        .base_timestamp = model::timestamp(1681991406158),
        .max_timestamp = model::timestamp(1681991406158),
        .delta_offset = model::offset_delta(86),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(29),
        .segment_term = model::term_id(28),
        .delta_offset_end = model::offset_delta(90),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(90),
        .committed_offset = model::offset(91),
        .base_timestamp = model::timestamp(1683198957316),
        .max_timestamp = model::timestamp(1683198957316),
        .delta_offset = model::offset_delta(90),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(30),
        .segment_term = model::term_id(29),
        .delta_offset_end = model::offset_delta(92),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(86),
        .committed_offset = model::offset(91),
        .base_timestamp = model::timestamp(1681991406158),
        .max_timestamp = model::timestamp(1683198957622),
        .delta_offset = model::offset_delta(86),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(30),
        .segment_term = model::term_id(28),
        .delta_offset_end = model::offset_delta(92),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(92),
        .committed_offset = model::offset(95),
        .base_timestamp = model::timestamp(1683199072652),
        .max_timestamp = model::timestamp(1683199072652),
        .delta_offset = model::offset_delta(92),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(31),
        .segment_term = model::term_id(30),
        .delta_offset_end = model::offset_delta(96),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(96),
        .committed_offset = model::offset(97),
        .base_timestamp = model::timestamp(1684266539007),
        .max_timestamp = model::timestamp(1684266539007),
        .delta_offset = model::offset_delta(96),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(32),
        .segment_term = model::term_id(31),
        .delta_offset_end = model::offset_delta(98),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(92),
        .committed_offset = model::offset(97),
        .base_timestamp = model::timestamp(1683199072652),
        .max_timestamp = model::timestamp(1684266539282),
        .delta_offset = model::offset_delta(92),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(32),
        .segment_term = model::term_id(30),
        .delta_offset_end = model::offset_delta(98),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1500,
        .base_offset = model::offset(98),
        .committed_offset = model::offset(101),
        .base_timestamp = model::timestamp(1684266633178),
        .max_timestamp = model::timestamp(1684266633178),
        .delta_offset = model::offset_delta(98),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(33),
        .segment_term = model::term_id(32),
        .delta_offset_end = model::offset_delta(102),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1239,
        .base_offset = model::offset(102),
        .committed_offset = model::offset(103),
        .base_timestamp = model::timestamp(1684266941322),
        .max_timestamp = model::timestamp(1684266941322),
        .delta_offset = model::offset_delta(102),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(34),
        .segment_term = model::term_id(33),
        .delta_offset_end = model::offset_delta(104),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2739,
        .base_offset = model::offset(98),
        .committed_offset = model::offset(103),
        .base_timestamp = model::timestamp(1684266633178),
        .max_timestamp = model::timestamp(1684266941649),
        .delta_offset = model::offset_delta(98),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(34),
        .segment_term = model::term_id(32),
        .delta_offset_end = model::offset_delta(104),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1432,
        .base_offset = model::offset(104),
        .committed_offset = model::offset(106),
        .base_timestamp = model::timestamp(1685032471788),
        .max_timestamp = model::timestamp(1685032471788),
        .delta_offset = model::offset_delta(104),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(35),
        .segment_term = model::term_id(34),
        .delta_offset_end = model::offset_delta(107),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1309,
        .base_offset = model::offset(107),
        .committed_offset = model::offset(109),
        .base_timestamp = model::timestamp(1685032567800),
        .max_timestamp = model::timestamp(1685032567800),
        .delta_offset = model::offset_delta(107),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(36),
        .segment_term = model::term_id(35),
        .delta_offset_end = model::offset_delta(110),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2741,
        .base_offset = model::offset(104),
        .committed_offset = model::offset(109),
        .base_timestamp = model::timestamp(1685032471788),
        .max_timestamp = model::timestamp(1685032831912),
        .delta_offset = model::offset_delta(104),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(36),
        .segment_term = model::term_id(34),
        .delta_offset_end = model::offset_delta(110),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1433,
        .base_offset = model::offset(110),
        .committed_offset = model::offset(112),
        .base_timestamp = model::timestamp(1685034809605),
        .max_timestamp = model::timestamp(1685034809605),
        .delta_offset = model::offset_delta(110),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(37),
        .segment_term = model::term_id(36),
        .delta_offset_end = model::offset_delta(113),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(113),
        .committed_offset = model::offset(114),
        .base_timestamp = model::timestamp(1685034837710),
        .max_timestamp = model::timestamp(1685034837710),
        .delta_offset = model::offset_delta(113),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(38),
        .segment_term = model::term_id(37),
        .delta_offset_end = model::offset_delta(115),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2673,
        .base_offset = model::offset(110),
        .committed_offset = model::offset(114),
        .base_timestamp = model::timestamp(1685034809605),
        .max_timestamp = model::timestamp(1685034838059),
        .delta_offset = model::offset_delta(110),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(38),
        .segment_term = model::term_id(36),
        .delta_offset_end = model::offset_delta(115),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1502,
        .base_offset = model::offset(115),
        .committed_offset = model::offset(118),
        .base_timestamp = model::timestamp(1685034919592),
        .max_timestamp = model::timestamp(1685034919592),
        .delta_offset = model::offset_delta(115),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(39),
        .segment_term = model::term_id(38),
        .delta_offset_end = model::offset_delta(119),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(119),
        .committed_offset = model::offset(120),
        .base_timestamp = model::timestamp(1685545469569),
        .max_timestamp = model::timestamp(1685545469569),
        .delta_offset = model::offset_delta(119),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(40),
        .segment_term = model::term_id(39),
        .delta_offset_end = model::offset_delta(121),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(115),
        .committed_offset = model::offset(120),
        .base_timestamp = model::timestamp(1685034919592),
        .max_timestamp = model::timestamp(1685545469763),
        .delta_offset = model::offset_delta(115),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(40),
        .segment_term = model::term_id(38),
        .delta_offset_end = model::offset_delta(121),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1502,
        .base_offset = model::offset(121),
        .committed_offset = model::offset(124),
        .base_timestamp = model::timestamp(1685545585591),
        .max_timestamp = model::timestamp(1685545585591),
        .delta_offset = model::offset_delta(121),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(41),
        .segment_term = model::term_id(40),
        .delta_offset_end = model::offset_delta(125),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(125),
        .committed_offset = model::offset(126),
        .base_timestamp = model::timestamp(1685545904346),
        .max_timestamp = model::timestamp(1685545904346),
        .delta_offset = model::offset_delta(125),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(42),
        .segment_term = model::term_id(41),
        .delta_offset_end = model::offset_delta(127),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(121),
        .committed_offset = model::offset(126),
        .base_timestamp = model::timestamp(1685545585591),
        .max_timestamp = model::timestamp(1685545904589),
        .delta_offset = model::offset_delta(121),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(42),
        .segment_term = model::term_id(40),
        .delta_offset_end = model::offset_delta(127),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1502,
        .base_offset = model::offset(127),
        .committed_offset = model::offset(130),
        .base_timestamp = model::timestamp(1686006281279),
        .max_timestamp = model::timestamp(1686006281279),
        .delta_offset = model::offset_delta(127),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(43),
        .segment_term = model::term_id(42),
        .delta_offset_end = model::offset_delta(131),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(131),
        .committed_offset = model::offset(132),
        .base_timestamp = model::timestamp(1686357788063),
        .max_timestamp = model::timestamp(1686357788063),
        .delta_offset = model::offset_delta(131),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(44),
        .segment_term = model::term_id(43),
        .delta_offset_end = model::offset_delta(133),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(127),
        .committed_offset = model::offset(132),
        .base_timestamp = model::timestamp(1686006281279),
        .max_timestamp = model::timestamp(1686357788409),
        .delta_offset = model::offset_delta(127),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(44),
        .segment_term = model::term_id(42),
        .delta_offset_end = model::offset_delta(133),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(133),
        .committed_offset = model::offset(134),
        .base_timestamp = model::timestamp(1687167031168),
        .max_timestamp = model::timestamp(1687167031168),
        .delta_offset = model::offset_delta(133),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(45),
        .segment_term = model::term_id(44),
        .delta_offset_end = model::offset_delta(135),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 193,
        .base_offset = model::offset(135),
        .committed_offset = model::offset(135),
        .base_timestamp = model::timestamp(1687167034917),
        .max_timestamp = model::timestamp(1687167034917),
        .delta_offset = model::offset_delta(135),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(45),
        .segment_term = model::term_id(44),
        .delta_offset_end = model::offset_delta(136),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1433,
        .base_offset = model::offset(133),
        .committed_offset = model::offset(135),
        .base_timestamp = model::timestamp(1687167031168),
        .max_timestamp = model::timestamp(1687167034917),
        .delta_offset = model::offset_delta(133),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(45),
        .segment_term = model::term_id(44),
        .delta_offset_end = model::offset_delta(136),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1634,
        .base_offset = model::offset(136),
        .committed_offset = model::offset(140),
        .base_timestamp = model::timestamp(1687167155032),
        .max_timestamp = model::timestamp(1687167155032),
        .delta_offset = model::offset_delta(136),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(46),
        .segment_term = model::term_id(45),
        .delta_offset_end = model::offset_delta(141),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(141),
        .committed_offset = model::offset(142),
        .base_timestamp = model::timestamp(1687263107314),
        .max_timestamp = model::timestamp(1687263107314),
        .delta_offset = model::offset_delta(141),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(47),
        .segment_term = model::term_id(46),
        .delta_offset_end = model::offset_delta(143),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2874,
        .base_offset = model::offset(136),
        .committed_offset = model::offset(142),
        .base_timestamp = model::timestamp(1687167155032),
        .max_timestamp = model::timestamp(1687263107596),
        .delta_offset = model::offset_delta(136),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(47),
        .segment_term = model::term_id(45),
        .delta_offset_end = model::offset_delta(143),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1433,
        .base_offset = model::offset(143),
        .committed_offset = model::offset(145),
        .base_timestamp = model::timestamp(1687263221342),
        .max_timestamp = model::timestamp(1687263221342),
        .delta_offset = model::offset_delta(143),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(48),
        .segment_term = model::term_id(47),
        .delta_offset_end = model::offset_delta(146),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1309,
        .base_offset = model::offset(146),
        .committed_offset = model::offset(148),
        .base_timestamp = model::timestamp(1687263251422),
        .max_timestamp = model::timestamp(1687263251422),
        .delta_offset = model::offset_delta(146),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(49),
        .segment_term = model::term_id(48),
        .delta_offset_end = model::offset_delta(149),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(143),
        .committed_offset = model::offset(148),
        .base_timestamp = model::timestamp(1687263221342),
        .max_timestamp = model::timestamp(1687263515465),
        .delta_offset = model::offset_delta(143),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(49),
        .segment_term = model::term_id(47),
        .delta_offset_end = model::offset_delta(149),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(149),
        .committed_offset = model::offset(150),
        .base_timestamp = model::timestamp(1688033577948),
        .max_timestamp = model::timestamp(1688033577948),
        .delta_offset = model::offset_delta(149),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(50),
        .segment_term = model::term_id(49),
        .delta_offset_end = model::offset_delta(151),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 193,
        .base_offset = model::offset(151),
        .committed_offset = model::offset(151),
        .base_timestamp = model::timestamp(1688033591235),
        .max_timestamp = model::timestamp(1688033591235),
        .delta_offset = model::offset_delta(151),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(50),
        .segment_term = model::term_id(49),
        .delta_offset_end = model::offset_delta(152),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1433,
        .base_offset = model::offset(149),
        .committed_offset = model::offset(151),
        .base_timestamp = model::timestamp(1688033577948),
        .max_timestamp = model::timestamp(1688033591235),
        .delta_offset = model::offset_delta(149),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(50),
        .segment_term = model::term_id(49),
        .delta_offset_end = model::offset_delta(152),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 8357,
        .base_offset = model::offset(152),
        .committed_offset = model::offset(161),
        .base_timestamp = model::timestamp(1688033700283),
        .max_timestamp = model::timestamp(1688033700283),
        .delta_offset = model::offset_delta(152),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(51),
        .segment_term = model::term_id(50),
        .delta_offset_end = model::offset_delta(162),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(162),
        .committed_offset = model::offset(163),
        .base_timestamp = model::timestamp(1688150074142),
        .max_timestamp = model::timestamp(1688150074142),
        .delta_offset = model::offset_delta(162),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(52),
        .segment_term = model::term_id(51),
        .delta_offset_end = model::offset_delta(164),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 9597,
        .base_offset = model::offset(152),
        .committed_offset = model::offset(163),
        .base_timestamp = model::timestamp(1688033700283),
        .max_timestamp = model::timestamp(1688150074433),
        .delta_offset = model::offset_delta(152),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(52),
        .segment_term = model::term_id(50),
        .delta_offset_end = model::offset_delta(164),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1433,
        .base_offset = model::offset(164),
        .committed_offset = model::offset(166),
        .base_timestamp = model::timestamp(1689089696113),
        .max_timestamp = model::timestamp(1689089696113),
        .delta_offset = model::offset_delta(164),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(53),
        .segment_term = model::term_id(52),
        .delta_offset_end = model::offset_delta(167),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1309,
        .base_offset = model::offset(167),
        .committed_offset = model::offset(169),
        .base_timestamp = model::timestamp(1689089771843),
        .max_timestamp = model::timestamp(1689089771843),
        .delta_offset = model::offset_delta(167),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(54),
        .segment_term = model::term_id(53),
        .delta_offset_end = model::offset_delta(170),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 1433,
        .base_offset = model::offset(164),
        .committed_offset = model::offset(166),
        .base_timestamp = model::timestamp(1689089696113),
        .max_timestamp = model::timestamp(1689089709407),
        .delta_offset = model::offset_delta(164),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(54),
        .segment_term = model::term_id(52),
        .delta_offset_end = model::offset_delta(167),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1433,
        .base_offset = model::offset(170),
        .committed_offset = model::offset(172),
        .base_timestamp = model::timestamp(1689089801939),
        .max_timestamp = model::timestamp(1689089801939),
        .delta_offset = model::offset_delta(170),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(55),
        .segment_term = model::term_id(54),
        .delta_offset_end = model::offset_delta(173),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(167),
        .committed_offset = model::offset(172),
        .base_timestamp = model::timestamp(1689089771843),
        .max_timestamp = model::timestamp(1689089802656),
        .delta_offset = model::offset_delta(167),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(55),
        .segment_term = model::term_id(53),
        .delta_offset_end = model::offset_delta(173),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1502,
        .base_offset = model::offset(173),
        .committed_offset = model::offset(176),
        .base_timestamp = model::timestamp(1689089897201),
        .max_timestamp = model::timestamp(1689089897201),
        .delta_offset = model::offset_delta(173),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(56),
        .segment_term = model::term_id(55),
        .delta_offset_end = model::offset_delta(177),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(177),
        .committed_offset = model::offset(178),
        .base_timestamp = model::timestamp(1689129812996),
        .max_timestamp = model::timestamp(1689129812996),
        .delta_offset = model::offset_delta(177),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(57),
        .segment_term = model::term_id(56),
        .delta_offset_end = model::offset_delta(179),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(173),
        .committed_offset = model::offset(178),
        .base_timestamp = model::timestamp(1689089897201),
        .max_timestamp = model::timestamp(1689129813366),
        .delta_offset = model::offset_delta(173),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(57),
        .segment_term = model::term_id(55),
        .delta_offset_end = model::offset_delta(179),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1502,
        .base_offset = model::offset(179),
        .committed_offset = model::offset(182),
        .base_timestamp = model::timestamp(1689364073367),
        .max_timestamp = model::timestamp(1689364073367),
        .delta_offset = model::offset_delta(179),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(58),
        .segment_term = model::term_id(57),
        .delta_offset_end = model::offset_delta(183),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = false,
        .size_bytes = 1240,
        .base_offset = model::offset(183),
        .committed_offset = model::offset(184),
        .base_timestamp = model::timestamp(1689366093051),
        .max_timestamp = model::timestamp(1689366093051),
        .delta_offset = model::offset_delta(183),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(59),
        .segment_term = model::term_id(58),
        .delta_offset_end = model::offset_delta(185),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
      cloud_storage::segment_meta{
        .is_compacted = true,
        .size_bytes = 2742,
        .base_offset = model::offset(179),
        .committed_offset = model::offset(184),
        .base_timestamp = model::timestamp(1689364073367),
        .max_timestamp = model::timestamp(1689366093327),
        .delta_offset = model::offset_delta(179),
        .ntp_revision = model::initial_revision_id(64),
        .archiver_term = model::term_id(59),
        .segment_term = model::term_id(57),
        .delta_offset_end = model::offset_delta(185),
        .sname_format = cloud_storage::segment_name_format(2),
        .metadata_size_hint = 0},
	};
    co_await start();
    ss::abort_source never_abort;
    auto res = co_await with_leader(
      10s, [this, &metas, &never_abort](raft::raft_node_instance&) {
          return get_leader_stm().add_segments(
            metas,
            std::nullopt,
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    ASSERT_TRUE_CORO(!res);
}

TEST_F_CORO(
  archival_metadata_stm_gtest_fixture,
  test_same_term_sync_pending_replication_failure) {
    /*
     * Similar to the previous test, but in this case the injected replication
     * delay is enough for leadership to reliably move and cause the replication
     * to error. Sync will fail in this case.
     */
    ss::abort_source never_abort;

    std::vector<cloud_storage::segment_meta> m;
    m.push_back(segment_meta{
      .base_offset = model::offset(0),
      .committed_offset = model::offset(99),
      .archiver_term = model::term_id(1),
      .segment_term = model::term_id(1)});

    co_await start();

    auto res = co_await with_leader(
      10s, [this, &m, &never_abort](raft::raft_node_instance&) {
          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    ASSERT_TRUE_CORO(!res);

    auto [plagued_node, delay_applied] = co_await with_leader(
      10s, [](raft::raft_node_instance& node) {
          raft::response_delay fail{
            .length = 5s, .on_applied = ss::promise<>{}};

          auto delay_applied = fail.on_applied->get_future();
          node.inject_failure(raft::msg_type::append_entries, std::move(fail));
          return std::make_tuple(node.get_vnode(), std::move(delay_applied));
      });

    m.clear();
    m.push_back(segment_meta{
      .base_offset = model::offset(100),
      .committed_offset = model::offset(199),
      .archiver_term = model::term_id(2),
      .segment_term = model::term_id(1)});

    auto slow_replication_fut = with_leader(
      10s,
      [this, &m, &never_abort, &plagued_node](raft::raft_node_instance& node) {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().add_segments(
            m,
            std::nullopt,
            ss::lowres_clock::now() + 10s,
            never_abort,
            cluster::segment_validated::yes);
      });

    co_await std::move(delay_applied);

    auto synced = co_await with_leader(
      10s, [this, &plagued_node](raft::raft_node_instance& node) mutable {
          if (node.get_vnode() != plagued_node) {
              throw std::runtime_error{"Leadership moved"};
          }

          return get_leader_stm().sync(10s);
      });

    ASSERT_FALSE_CORO(synced);

    auto slow_replication_res = co_await std::move(slow_replication_fut);
    ASSERT_TRUE_CORO(slow_replication_res);
}
