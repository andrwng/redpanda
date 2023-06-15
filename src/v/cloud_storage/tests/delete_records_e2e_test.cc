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
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage/tests/s3_imposter.h"
#include "config/configuration.h"
#include "kafka/server/tests/delete_records_utils.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "model/fundamental.h"
#include "redpanda/tests/fixture.h"
#include "storage/disk_log_impl.h"

#include <seastar/core/io_priority_class.hh>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/test/tools/old/interface.hpp>

#include <iterator>

using tests::kafka_consume_transport;
using tests::kafka_delete_records_transport;
using tests::kafka_produce_transport;
using tests::kv_t;

static ss::logger e2e_test_log("delete_records_e2e_test");

class delete_records_e2e_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    delete_records_e2e_fixture()
      : redpanda_thread_fixture(
        redpanda_thread_fixture::init_cloud_storage_tag{},
        httpd_port_number()) {
        // No expectations: tests will PUT and GET organically.
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        config::shard_local_cfg().log_compaction_interval_ms.set_value(
          std::chrono::duration_cast<std::chrono::milliseconds>(1s));
    }
};

FIXTURE_TEST(test_delete_local, delete_records_e2e_fixture) {
    const model::topic topic_name("tapioca");
    model::ntp ntp(model::kafka_namespace, topic_name, 0);
    cluster::topic_properties props;
    props.shadow_indexing = model::shadow_indexing_mode::full;
    props.retention_local_target_bytes = tristate<size_t>(1);
    props.cleanup_policy_bitflags = model::cleanup_policy_bitflags::deletion;
    add_topic({model::kafka_namespace, topic_name}, 1, props).get();
    wait_for_leader(ntp).get();

    // Do some sanity checks that our partition looks the way we expect (has a
    // log, archiver, etc).
    auto partition = app.partition_manager.local().get(ntp);
    auto* log = dynamic_cast<storage::disk_log_impl*>(
      partition->log().get_impl());
    auto archiver_ref = partition->archiver();
    BOOST_REQUIRE(archiver_ref.has_value());
    auto& archiver = archiver_ref.value().get();

    kafka_produce_transport producer(make_kafka_client().get());
    producer.start().get();
    std::vector<kv_t> records{
      {"key0", "val0"},
      {"key1", "val1"},
      {"key2", "val2"},
    };
    for (const auto& r : records) {
        producer.produce_to_partition(topic_name, model::partition_id(0), {r})
          .get();
    }
    log->flush().get();
    log->force_roll(ss::default_priority_class()).get();
    BOOST_REQUIRE_EQUAL(2, log->segments().size());

    // Upload the closed segment to object storage.
    tests::cooperative_spin_wait_with_timeout(3s, [&archiver] {
        return archiver.upload_next_candidates().then(
          [&](archival::ntp_archiver::batch_result) {
              return archiver.manifest().size() >= 1;
          });
    }).get();
    auto manifest_res = archiver.upload_manifest("test").get();
    BOOST_REQUIRE_EQUAL(manifest_res, cloud_storage::upload_result::success);
    archiver.flush_manifest_clean_offset().get();

    kafka_delete_records_transport deleter(make_kafka_client().get());
    deleter.start().get();
    auto lwm = deleter
                 .delete_records_from_partition(
                   topic_name, model::partition_id(0), model::offset(1), 5s)
                 .get();
    BOOST_CHECK_EQUAL(model::offset(1), lwm);
    tests::cooperative_spin_wait_with_timeout(3s, [&log] {
        return log->segment_count() == 1;
    }).get();

    kafka_consume_transport consumer(make_kafka_client().get());
    consumer.start().get();
    const auto check_consume_out_of_range = [&](model::offset kafka_offset) {
        BOOST_REQUIRE_EXCEPTION(
          consumer
            .consume_from_partition(
              topic_name, model::partition_id(0), kafka_offset)
            .get(),
          std::runtime_error,
          [](std::runtime_error e) {
              return std::string(e.what()).find("out_of_range")
                     != std::string::npos;
          });
    };
    auto consumed_records = consumer
                              .consume_from_partition(
                                topic_name,
                                model::partition_id(0),
                                model::offset(1))
                              .get();
    BOOST_CHECK_EQUAL(2, consumed_records.size());
    BOOST_CHECK_EQUAL("key1", consumed_records[0].first);
    BOOST_CHECK_EQUAL("val1", consumed_records[0].second);
    BOOST_CHECK_EQUAL("key2", consumed_records[1].first);
    BOOST_CHECK_EQUAL("val2", consumed_records[1].second);
    check_consume_out_of_range(model::offset(0));
}
