# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import time
from packaging.version import Version

from ducktape.mark import parametrize
from rptest.tests.partition_movement import PartitionMovementMixin
from ducktape.utils.util import wait_until
from rptest.clients.ping_pong import PingPong
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.prealloc_nodes import PreallocNodesTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.end_to_end import EndToEndTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
    KgoVerifierRandomConsumer,
    KgoVerifierConsumerGroupConsumer,
)
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller, wait_for_num_versions


# Repro test case for Lacework.
class EmptyNodeTest(PreallocNodesTest, PartitionMovementMixin):

    MSG_SIZE = 100
    PRODUCE_COUNT = 2500
    RANDOM_READ_COUNT = 100
    RANDOM_READ_PARALLEL = 4
    CONSUMER_GROUP_READERS = 4

    topics = (TopicSpec(partition_count=10, replication_factor=3), )

    def __init__(self, test_context):
        extra_rp_conf = \
            {
                "enable_idempotence": True,
                "election_timeout_ms": 5000,
                #"raft_learner_recovery_rate": 10,
                #"raft_max_concurrent_append_requests_per_follower": 3,
                "raft_heartbeat_interval_ms": 500,
            }
        super(EmptyNodeTest, self).__init__(test_context=test_context,
                                            extra_rp_conf=extra_rp_conf,
                                            cluster_config_in_rp_yaml=True,
                                            num_brokers=4,
                                            node_prealloc_count=1)
        self.installer = self.redpanda._installer
        self._producer = KgoVerifierProducer(test_context, self.redpanda,
                                             self.topic, self.MSG_SIZE,
                                             self.PRODUCE_COUNT,
                                             self.preallocated_nodes)
        self._seq_consumer = KgoVerifierSeqConsumer(test_context,
                                                    self.redpanda, self.topic,
                                                    self.MSG_SIZE,
                                                    self.preallocated_nodes)
        self._rand_consumer = KgoVerifierRandomConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.RANDOM_READ_COUNT, self.RANDOM_READ_PARALLEL,
            self.preallocated_nodes)
        self._cg_consumer = KgoVerifierConsumerGroupConsumer(
            test_context, self.redpanda, self.topic, self.MSG_SIZE,
            self.CONSUMER_GROUP_READERS, self.preallocated_nodes)

        self._consumers = [
            self._seq_consumer, self._rand_consumer, self._cg_consumer
        ]

    def setUp(self):
        self.installer.install(self.redpanda.nodes, (22, 1, 7))
        super(EmptyNodeTest, self).setUp()

    @cluster(num_nodes=5)
    def test_start_empty(self):
        admin = self.redpanda._admin
        pp = PingPong(self.redpanda.brokers_list(), self.topic, 0, self.logger)
        pp.ping_pong(timeout_s=30)
        admin.wait_stable_configuration(
            topic="id_allocator", namespace="kafka_internal",
            timeout_s=30)

        self._producer.start(clean=False)
        self._producer.wait_for_offset_map()
        self._producer.produce_status.acked
        for consumer in self._consumers:
            consumer.start(clean=False)

        partition_details = admin.wait_stable_configuration(
            topic="id_allocator", namespace="kafka_internal",
            timeout_s=30)
        id_allocator_node_ids = partition_details.replicas
        assert len(id_allocator_node_ids) == 1, \
            f"Expected 1 replica, got {partition_details.replicas}"

        initial_node_id = next(iter(id_allocator_node_ids))
        self.logger.warn(f"Initial node ID: {initial_node_id}")

        def bounded_node_id(node_id):
            bounded = node_id % 4
            if bounded == 0:
                return 4
            return bounded

        initial_three_assignment = []
        for i in range(3):
            initial_three_assignment.append(
                {"node_id": bounded_node_id(i + initial_node_id), "core": 1})
        self.logger.warn(f"Initial three placement: {initial_three_assignment}")

        admin.set_partition_replicas("id_allocator",
                                     0,
                                     initial_three_assignment,
                                     namespace="kafka_internal")
        def has_three_replicas():
            pd = admin.wait_stable_configuration(topic="id_allocator",
                                                 namespace="kafka_internal",
                                                 timeout_s=30)
            return len(pd.replicas) == 3
        wait_until(has_three_replicas, timeout_sec=30, backoff_sec=1)

        # We'll decommission a node to trigger a config change.
        node_to_decom = bounded_node_id(initial_node_id + 1)

        self.logger.warn(f"Decommissioning node {node_to_decom}")

        # Make the config change last some time by restarting the
        # 'id_allocator' leader and forcing a re-election.
        # Make sure we don't disrupt the controller though (force it to move
        # elsewhere), and make sure we placement on the node we're going to
        # wipe (the initial node). We'll do this by assigning leadership manually.
        id_allocator_leader_id = bounded_node_id(initial_node_id + 2)
        admin.transfer_leadership_to(namespace="kafka_internal",
                                     topic="id_allocator",
                                     partition=0,
                                     target_id=id_allocator_leader_id)
        admin.wait_stable_configuration(topic="id_allocator",
                                        namespace="kafka_internal",
                                        timeout_s=30)

        controller_leader_id = bounded_node_id(id_allocator_leader_id + 1)
        admin.transfer_leadership_to(namespace="redpanda",
                                     topic="controller",
                                     partition=0,
                                     target_id=controller_leader_id)
        admin.await_stable_leader("controller", partition=0, namespace="redpanda", timeout_s=30)

        id_allocator_leader = self.redpanda.get_node(id_allocator_leader_id)
        self.redpanda.restart_nodes(id_allocator_leader)

        # Hit the controller with a decommission request.
        admin.decommission_broker(node_to_decom,
                                  node=self.redpanda.get_node(controller_leader_id))

        # Restart the initial node with an empty disk, encouraging it to replay
        # and incorrectly apply controller operations. We should see this node
        # become leader of 'id_allocator' along the way, since it started as a
        # single-replica partition.
        initial_node = self.redpanda.get_node(initial_node_id)
        self.redpanda.stop_node(initial_node)
        self.redpanda.clean_node(initial_node,
                                 preserve_logs=False,
                                 preserve_current_install=True)
        self.redpanda.write_node_conf_file(initial_node)
        self.redpanda.start_node(initial_node)
        time.sleep(5)


        self._producer.wait()
        for consumer in self._consumers:
            consumer.wait()

