from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.raft_availability_test import RaftAvailabilityTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kafka_cli_producer import KafkaCliProducer
import time
from enum import Enum
from random import choice


# TODO: make RaftAvailabilityTest a mixin
class UpgradeAvailabilityTest(PartitionMovementMixin, RaftAvailabilityTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )

    def __init__(self, test_context):
        super(UpgradeAvailabilityTest,
              self).__init__(test_ctx=test_context,
                             num_brokers=4,
                             legacy_config_mode=True)

        self.consumer = None
        self.producer = None

    def setUp(self):

        # Condition for local development
        if not self.redpanda.dedicated_nodes:
            # Make sure each RP node has the list of updated
            # redanda packages
            for node in self.redpanda.nodes:
                cmd = "curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash"
                node.account.ssh(cmd, allow_fail=False)

        # Use 21.11.15 at startup
        cmd = 'sudo apt -o  Dpkg::Options::="--force-confnew" install -y --allow-downgrades redpanda=21.11.15-1-7325762b'
        for node in self.redpanda.nodes:
            for line in node.account.ssh_capture(cmd, allow_fail=False):
                line = line.strip()
                self.logger.info(f'{node.name} apt result: {line}')

        super().setUp()  # RP nodes start in super

    def restart_node_w_version(self, node, version: str):
        self.redpanda.stop_node(node, timeout=300)
        cmd = f'sudo apt -o  Dpkg::Options::="--force-confnew" install -y --allow-downgrades redpanda={version}'
        node.account.ssh(cmd, allow_fail=False)
        # node.account.ssh("sudo systemctl stop redpanda")
        self.logger.info("Restarting node with new version")
        self.redpanda.start_node(node, None, timeout=300)

    def restart_nodes_w_version(self, version: str):
        for node in self.redpanda.nodes:
            self.restart_node_w_version(node, version)

    # Each producer writes 600Mbps and the consumers read until completion.
    def start_workload(self, runtime: int):
        if self.producer is not None:
            raise RuntimeError('Producer is already defined')

        self.producer = KafkaCliProducer(self.test_context,
                                         self.redpanda,
                                         self.topic,
                                         msg_size_mb=10,
                                         runtime=runtime,
                                         compression=True,
                                         kafka_version='2.8.0')
        self.producer.start()

    def wait_on_workload(self):
        # Terminate when the producer finishes
        self.producer.wait()


    class RestartTarget(Enum):
        LEADER = 1
        MOVE_DEST = 2

    class MoveTarget(Enum):
        LEADER = 1
        FOLLOWER = 2

    def move_and_upgrade(self, restart_target, move_target):
        admin = Admin(self.redpanda)

        # Start workload
        self.start_workload(runtime=20)

        leader_id = self._wait_for_leader()[0]
        orig_assignments = self._get_assignments(admin, self.topic, 0)
        orig_node_ids = [a["node_id"] for a in orig_assignments]
        brokers = admin.get_brokers()

        # Wait for there to be substantial load.
        time.sleep(30)

        empty_node_ids = [b["node_id"] for b in brokers if b["node_id"] not in orig_node_ids]

        src_node_id = -1
        if move_target == self.MoveTarget.LEADER:
            src_node_id = leader_id
        elif move_target == self.MoveTarget.FOLLOWER:
            src_node_id = leader_id
            while src_node_id == leader_id:
                src_node_id = choice(orig_node_ids)

        dst_node_id = empty_node_ids[0]
        self.logger.info("Moving replica from node {src_node_id} to {dst_node_id}")
        assignments = self._move_replica(self.topic, 0, src_node_id, dst_node_id)

        # Upgrade the leader node to 22.1.3
        node_id_to_upgrade = None
        if restart_target == self.RestartTarget.LEADER:
            node_id_to_upgrade = leader_id
        elif restart_target == self.RestartTarget.MOVE_DEST:
            node_id_to_upgrade = dst_node_id
        self.logger.info("Restarting node {node_id_to_upgrade}")
        node_to_upgrade = self.redpanda.get_node(node_id_to_upgrade)
        self.restart_node_w_version(node_to_upgrade, '22.1.3-1')

        self._wait_post_move(self.topic, 0, assignments, 30)

        self.wait_on_workload()


    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_upgrade_leader_move_leader(self):
        self.move_and_upgrade(self.RestartTarget.LEADER, self.MoveTarget.LEADER)

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_upgrade_leader_move_follower(self):
        self.move_and_upgrade(self.RestartTarget.LEADER, self.MoveTarget.FOLLOWER)

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_upgrade_dst_move_leader(self):
        self.move_and_upgrade(self.RestartTarget.MOVE_DEST, self.MoveTarget.LEADER)

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_upgrade_dst_move_follower(self):
        self.move_and_upgrade(self.RestartTarget.MOVE_DEST, self.MoveTarget.FOLLOWER)
