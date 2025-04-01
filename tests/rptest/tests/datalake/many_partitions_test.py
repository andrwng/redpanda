# Copyright 2025 Redpanda Data, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

from concurrent.futures import ThreadPoolExecutor
from ducktape.mark import matrix
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.services.catalog_service import CatalogType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda import PandaproxyConfig, SchemaRegistryConfig, SISettings
from rptest.services.redpanda_connect import RedpandaConnectService
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.datalake_verifier import DatalakeVerifier
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.utils.rpcn_utils import counter_stream_config
from typing import Any


class DatalakeManyPartitionsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        super(DatalakeManyPartitionsTest,
              self).__init__(test_ctx,
                             num_brokers=4,
                             si_settings=SISettings(test_context=test_ctx),
                             extra_rp_conf={
                                 "iceberg_enabled": "true",
                                 "iceberg_catalog_commit_interval_ms": 1000
                             },
                             schema_registry_config=SchemaRegistryConfig(),
                             pandaproxy_config=PandaproxyConfig(),
                             *args,
                             **kwargs)
        self.test_ctx = test_ctx
        self.topic_name = "test"
        self.rpcn = RedpandaConnectService(self.test_context, self.redpanda)

    def setUp(self):
        # NOTE: defer cluster startup to DatalakeServices.
        self.rpcn.start()

    def start_unstructured_topic_stream(
        self,
        dl: DatalakeServices,
        topic: str,
        replicas: int = 1,
        partitions: int = 1,
        create_topic: bool = True,
        topic_config: dict[str, Any] = dict()) -> str:
        """
        Creates a RPCN stream ingesting to non-structured the given Iceberg
        topic, optionally creating the topic. Returns the stream name.
        """
        # Send a low volume of records. We don't want to overwhelm the cluster.
        cfg = counter_stream_config(
            self.redpanda,
            topic,
            "",  # subject
            cnt=0,  # indefinite count
            interval_ms=1)
        if create_topic:
            dl.create_iceberg_enabled_topic(topic,
                                            replicas=replicas,
                                            partitions=partitions,
                                            iceberg_mode="key_value",
                                            config=topic_config)
        stream = f"{topic}_stream"
        self.rpcn.start_stream(name=stream, config=cfg)
        dl.wait_for_iceberg_table("redpanda", topic, timeout=30, backoff_sec=1)
        return stream

    @cluster(num_nodes=7, log_allow_list=["UpdateRequirement\\$Assert"])
    @matrix(cloud_storage_type=supported_storage_types())
    def test_many_partitions(self, cloud_storage_type):
        with DatalakeServices(self.test_context,
                              redpanda=self.redpanda,
                              include_query_engines=[QueryEngineType.SPARK],
                              catalog_type=CatalogType.REST_JDBC) as dl:
            max_lag_prop_name = "redpanda.iceberg.target.lag.ms"

            replicas_per_cpu = 5000 if self.redpanda.dedicated_nodes else 100
            num_topics = 10 if self.redpanda.dedicated_nodes else 6
            num_nodes = len(self.redpanda.nodes)
            cpus_per_node = self.redpanda.get_node_cpu_count()
            rf = 3
            total_replicas = replicas_per_cpu * cpus_per_node * num_nodes
            partitions_per_topic = int((total_replicas / num_topics) / rf)

            # NOTE: in docker, 133 partitions.
            self.redpanda.logger.info(
                f"Creating topics with {partitions_per_topic} partitions for {total_replicas} replicas total"
            )

            streams = []
            topics = []
            for i in range(int(num_topics / 2)):
                fast_topic = f"rapidash_{i}"
                slow_topic = f"slowpoke_{i}"
                topics.append(fast_topic)
                topics.append(slow_topic)
                streams.append(
                    self.start_unstructured_topic_stream(
                        dl,
                        fast_topic,
                        replicas=rf,
                        partitions=partitions_per_topic,
                        topic_config={max_lag_prop_name: 10000}))
                streams.append(
                    self.start_unstructured_topic_stream(
                        dl,
                        slow_topic,
                        replicas=rf,
                        partitions=partitions_per_topic,
                        topic_config={max_lag_prop_name: 30000}))

            spark = dl.spark()

            def table_max_offsets_by_partition(topic) -> dict[int, int]:
                max_offsets_query =  \
                  "select redpanda.partition, max(redpanda.offset) " \
                  f"from redpanda.{topic} " \
                  "group by redpanda.partition " \
                  "order by redpanda.partition"
                return dict(spark.run_query_fetch_all(max_offsets_query))

            def kafka_max_offsets_by_partition(topic) -> dict[int, int]:
                rpk = RpkTool(self.redpanda)
                return dict([(p.id, p.high_watermark - 1)
                             for p in rpk.describe_topic(topic)
                             if p.high_watermark > 0])

            def translated_ge(topic,
                              target_offsets: dict[int, int],
                              target_extra=0):
                current_offsets = table_max_offsets_by_partition(topic)
                partitions_missing = []
                failed_conditions = []
                for target_p, target_o in target_offsets.items():
                    if target_p not in current_offsets:
                        partitions_missing.append(target_p)
                        continue
                    final_target_o = target_o + target_extra
                    current_o = current_offsets[target_p]
                    if current_o < final_target_o:
                        failed_conditions.append(
                            f"{target_p}: {current_o} < {final_target_o}")
                        continue
                if len(partitions_missing) == 0 and len(
                        failed_conditions) == 0:
                    return True

                self.redpanda.logger.debug(
                    f"[{topic}] missing partitions: {partitions_missing}, failed conditions: {failed_conditions}"
                )
                return False

            def all_partitions_translated(topic, partitions, count):
                offset_target = count - 1
                target_offsets = dict([(p, offset_target)
                                       for p in range(partitions)])
                return translated_ge(topic, target_offsets)

            wait_timeout = 480 if self.redpanda.dedicated_nodes else 120

            def wait_until_initial_translated(topic):
                wait_until(lambda: all_partitions_translated(
                    topic, partitions_per_topic, 1),
                           timeout_sec=wait_timeout,
                           backoff_sec=1)

            def wait_until_hwm_translated(topic):
                snap = kafka_max_offsets_by_partition(topic)
                wait_until(lambda: translated_ge(topic, snap),
                           timeout_sec=wait_timeout,
                           backoff_sec=1)

            self.redpanda.logger.info("Beginning wait for translation")
            with ThreadPoolExecutor() as executor:
                futs = []
                for t in topics:
                    futs.append(
                        executor.submit(wait_until_initial_translated, t))
                for f in futs:
                    f.result()
            self.redpanda.logger.info("Completed wait for translation")

            self.redpanda.logger.info("Performing rolling restart")
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes)
            self.redpanda.logger.info("Completed rolling restart")

            self.redpanda.logger.info(
                "Beginning wait for translation post restart")
            with ThreadPoolExecutor() as executor:
                futs = []
                for t in topics:
                    futs.append(executor.submit(wait_until_hwm_translated, t))
                for f in futs:
                    f.result()
            self.redpanda.logger.info(
                "Completed wait for translation post restart")

            self.redpanda.logger.info("Stopping RPCN")
            self.rpcn.stop()
            self.redpanda.logger.info(
                "Beginning wait for translation after stopping RPCN")
            with ThreadPoolExecutor() as executor:
                futs = []
                for t in topics:
                    futs.append(executor.submit(wait_until_hwm_translated, t))
                for f in futs:
                    f.result()
            self.redpanda.logger.info(
                "Completed wait for translation after stopping RPCN")

            self.redpanda.logger.info("Running validators")
            with ThreadPoolExecutor() as executor:
                futs = []
                for t in topics:
                    futs.append(
                        executor.submit(DatalakeVerifier.oneshot,
                                        self.redpanda,
                                        t,
                                        spark,
                                        progress_timeout_sec=60,
                                        total_timeout=wait_timeout))
                for f in futs:
                    f.result()
