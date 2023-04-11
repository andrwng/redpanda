# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import random

from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer, KgoVerifierRandomConsumer
from rptest.services.redpanda import SISettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool, RpkException
from ducktape.utils.util import wait_until, TimeoutError


class HardRestartTest(PreallocNodesTest):
    small_segment_size = 1024 * 1024

    def __init__(self, test_context):
        extra_rp_conf = {
            "enable_leader_balancer": False,
        }
        si_settings = SISettings(test_context,
                                 log_segment_size=self.small_segment_size)
        super(HardRestartTest, self).__init__(test_context=test_context,
                                              num_brokers=3,
                                              node_prealloc_count=1,
                                              si_settings=si_settings,
                                              extra_rp_conf=extra_rp_conf)
        self.rpk = RpkTool(self.redpanda)

    def elections_done(self, topic_name, num_partitions):
        try:
            partitions = list(
                self.rpk.describe_topic(topic_name, tolerant=True))
        except RpkException:
            return False
        if len(partitions) < num_partitions:
            self.logger.info(
                f"describe only includes {len(partitions)} partitions for {topic_name}"
            )
            self.logger.debug(
                f"waiting for {num_partitions} partitions: {partitions}")
            return False
        return True

    @cluster(num_nodes=4)
    def test_restart_during_truncations(self):
        """
        - write to a given partition
        - kill the leader of the partition
        - restart the node briefly, allowing for some truncations
        - kill the node mid-truncation
        - restart the node and make sure all nodes can become available
        """
        topic_name = "tapioca"
        other_topic_name = "distractor"
        num_partitions = 5
        config = {
            "segment.bytes": self.small_segment_size,
            "retention.bytes": -1,
        }
        self.rpk.create_topic(other_topic_name,
                              partitions=128 - num_partitions,
                              replicas=3,
                              config=config)
        self.rpk.create_topic(topic_name,
                              partitions=num_partitions,
                              replicas=3,
                              config=config)
        target_node = self.redpanda.nodes[0]
        target_node_id = self.redpanda.node_id(target_node)
        wait_until(lambda: self.elections_done(topic_name, num_partitions),
                   timeout_sec=30,
                   backoff_sec=1)
        for i in range(num_partitions):
            self.redpanda._admin.transfer_leadership_to(
                namespace="kafka",
                topic=topic_name,
                partition=i,
                target_id=target_node_id)

        msg_size = 32 * 1024
        # Write enough such that we have more than one segment per partition.
        # ~3K msgs ~= 96MiB
        msg_count = int(
            (num_partitions * self.small_segment_size * 3) / msg_size)
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            msg_count + 10000000,
            custom_node=[self.preallocated_nodes[0]])
        try:
            producer.start()
            wait_until(lambda: producer.produce_status.acked > msg_count,
                       timeout_sec=30,
                       backoff_sec=0.05)
            # Isolate the leader so it can attempt replication to other nodes
            # and fail. This allows some records to only go into the target
            # node, teeing up for truncation.
            self.redpanda.stop_node(self.redpanda.nodes[1])
            self.redpanda.stop_node(self.redpanda.nodes[2])
            time.sleep(10)
        finally:
            producer.stop()
            producer.wait(timeout_sec=300)
            producer.clean()

        # Bring the other nodes up and bring this node down. They should elect
        # a leader and continue replicating in a new term.
        self.redpanda.stop_node(target_node)
        self.redpanda.start_node(self.redpanda.nodes[1])
        self.redpanda.start_node(self.redpanda.nodes[2])
        wait_until(lambda: self.elections_done(topic_name, num_partitions),
                   timeout_sec=30,
                   backoff_sec=1)
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size,
            msg_count,
            custom_node=[self.preallocated_nodes[0]])
        try:
            producer.start()

            # Restart the target node. It should attempt to truncate, but send
            # a SIGKILL to exercise crash consistency.
            self.redpanda.start_node(target_node, skip_readiness_check=True)
            time.sleep(random.randint(0, 10 * 1000) / 1000)
            self.redpanda.stop_node(target_node, forced=True)
        finally:
            producer.stop()
            producer.wait(timeout_sec=300)
            producer.clean()

        # Does the server get up without errors?
        drop_caches_cmd = "echo 3 > /proc/sys/vm/drop_caches"
        target_node.account.ssh_output(drop_caches_cmd)
        self.redpanda.start_node(target_node)
        wait_until(self.redpanda.healthy, timeout_sec=30, backoff_sec=1)
        assert self.redpanda.search_log_any("failed to create") is False
