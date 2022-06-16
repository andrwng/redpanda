from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.tests.partition_movement import PartitionMovementMixin
from rptest.tests.raft_availability_test import RaftAvailabilityTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kafka_simple_producer import KafkaSimpleProducer
import time
from enum import Enum
from random import choice
import threading


# TODO: make RaftAvailabilityTest a mixin
class UpgradeAvailabilityTest(PartitionMovementMixin, RaftAvailabilityTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3), )
    class RestartTarget(Enum):
        LEADER = 1
        MOVE_DEST = 2

    class MoveTarget(Enum):
        LEADER = 1
        FOLLOWER = 2

    def __init__(self, test_context):
        super(UpgradeAvailabilityTest, self).__init__(test_ctx=test_context,
                                                      num_brokers=4,
                                                      legacy_config_mode=True)

        self.consumer = None
        self.producer = None
        self.assignments = None
        self.admin = Admin(self.redpanda)
        self.rp_hostnames = [
            node.account.hostname for node in self.redpanda.nodes
        ]

    def setUp(self):

        # Condition for local development
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

    def restart_node_w_version(self,
                               node,
                               version: str,
                               override_cfg_params=None):
        self.redpanda.stop_node(node, timeout=300)
        cmd = f'sudo apt -o  Dpkg::Options::="--force-confnew" install -y --allow-downgrades redpanda={version}'
        node.account.ssh(cmd, allow_fail=False)
        # node.account.ssh("sudo systemctl stop redpanda")
        self.logger.info("Restarting node with new version")
        self.redpanda.start_node(node, override_cfg_params, timeout=300)

    def restart_nodes_w_version(self, version: str):
        for node in self.redpanda.nodes:
            self.restart_node_w_version(node, version)

    def start_workload(self, runtime: int):
        if self.producer is not None:
            raise RuntimeError('Producer is already defined')

        # The simple producer uses Kafka 2.8.0
        self.producer = KafkaSimpleProducer(self.test_context,
                                            self.redpanda,
                                            self.topic,
                                            runtime=runtime)
        self.producer.start()

    def move_from_src_to_dest(self, src_id: int, dest_id: int):
        self.logger.info(f'Moving replica from node {src_id} to {dest_id}')
        self.assignments = self._move_replica(self.topic, 0, src_id, dest_id)

    def run_workload_and_get_replica_sets(self):
        # Start workload
        self.start_workload(runtime=180)

        orig_assignments = self._get_assignments(self.admin, self.topic, 0)
        nodes_in_replica_set = [a["node_id"] for a in orig_assignments]
        brokers = self.admin.get_brokers()

        # Wait for some load
        wait_until(lambda: self.producer.record_count > 200000,
                   timeout_sec=180,
                   err_msg='Failed to write enough data')

        nodes_not_in_replica_set = [
            b["node_id"] for b in brokers
            if b["node_id"] not in nodes_in_replica_set
        ]

        return nodes_in_replica_set, nodes_not_in_replica_set

    def confirm_partition_movement_status(self,
                                          hosts: list[str],
                                          target_status: str,
                                          timeout_s: int = 120):
        def check_status_on_all_replicas():
            replicas_status = []

            for host in hosts:
                self.logger.info(f'Movement status check: {host} {target_status}')
                res = self.admin._get_configuration(host,
                                                    namespace='kafka',
                                                    topic=self.topic,
                                                    partition=0)

                if 'status' not in res:
                    return False
                else:
                    status = res['status']
                    self.logger.info(f'Movement status: {status}')
                    replicas_status.append(status == target_status)

            self.logger.info(f'Movement status check: {target_status} {all(replicas_status)}')
            return all(replicas_status)

        wait_until(check_status_on_all_replicas, timeout_sec=timeout_s)

    def restart_node_and_wait_partition_movement(self, node_id: int):
        record_size = 512
        raft_recovery_default_read_size = record_size * 1.5

        extra_rp_conf = {
            "enable_leader_balancer": False,
            "id_allocator_replication": 3,
            "log_segment_size": 1048576,

            # raft_recovery_default_read_size is available on 22.1.1 and newer
            "raft_recovery_default_read_size": raft_recovery_default_read_size,
        }

        self.logger.info(f'Restarting node {node_id} with version 22.1.3')
        node_to_upgrade = self.redpanda.get_node(node_id)
        self.restart_node_w_version(node_to_upgrade,
                                    '22.1.3-1',
                                    override_cfg_params=extra_rp_conf)

        # Make sure partition movement is still in progress
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')

        # Make sure the producer is still writing
        records_written_after_upgrade = self.producer.record_count
        wait_until(
            lambda: self.producer.record_count > records_written_after_upgrade,
            timeout_sec=60,
            err_msg='Producer stopped writing')

        # Wait until all replicas have the same data
        self.admin.wait_stable_configuration(topic=self.topic,
                                             namespace='kafka',
                                             replication=3,
                                             timeout_s=120)

        # Wait until movement status is done on all nodes
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'done',
                                               timeout_s=300)

        # Terminate when the producer finishes
        self.producer.wait(timeout_sec=1200)

    def run_partition_movement_for_n_seconds(self,
                                             hosts: list[str],
                                             timeout_s: int = 10):
        time.sleep(timeout_s)
        self.confirm_partition_movement_status(hosts, "in_progress")

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_node_will_not_be_in_replica_set(self):
        rs_tuple = self.run_workload_and_get_replica_sets()
        nodes_in_replica_set = rs_tuple[0]
        nodes_not_in_replica_set = rs_tuple[1]

        # Execute partition movement such that node X will no longer be in the
        # replica set. Therefore, move the partition to a node that is not in
        # the replica set.
        node_x = choice(nodes_in_replica_set)
        node_y = choice(nodes_not_in_replica_set)
        self.logger.info(f'Move partition from {node_x} to {node_y}')
        part_movement_th = threading.Thread(
            target=self.move_from_src_to_dest,
            args=(node_x, node_y),
        )
        part_movement_th.start()

        # Check that partition movement started/is on-going
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')

        self.run_partition_movement_for_n_seconds(self.rp_hostnames,
                                                  timeout_s=1)

        # After node restart, probably within the below function,
        # again check that parttion movement started/is on-going
        self.restart_node_and_wait_partition_movement(node_id=node_x)
        part_movement_th.join()

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_node_will_be_in_replica_set(self):
        rs_tuple = self.run_workload_and_get_replica_sets()
        nodes_in_replica_set = rs_tuple[0]
        nodes_not_in_replica_set = rs_tuple[1]

        # Execute partition movement such that node X will be in the replica
        # set. Therefore, move the partition from a node in the replica set to
        # node X.
        node_x = choice(nodes_not_in_replica_set)
        node_y = choice(nodes_in_replica_set)
        self.logger.info(f'Move partition from {node_y} to {node_x}')
        part_movement_th = threading.Thread(
            target=self.move_from_src_to_dest,
            args=(node_y, node_x),
        )
        part_movement_th.start()

        # Check that partition movement started/is on-going
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')
        self.logger.info("AWONG finished waiting for in progress")

        self.run_partition_movement_for_n_seconds(self.rp_hostnames,
                                                  timeout_s=1)

        self.restart_node_and_wait_partition_movement(node_id=node_x)
        part_movement_th.join()

    @cluster(num_nodes=5, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def test_node_will_remain_in_replica_set(self):
        rs_tuple = self.run_workload_and_get_replica_sets()
        nodes_in_replica_set = rs_tuple[0]
        nodes_not_in_replica_set = rs_tuple[1]

        # Execute partition movement such that node X remains in the replica
        # set. Therefore, move the partition from another node (not node X)
        # in the replica set to a node not in the replica set.
        node_x = choice(nodes_in_replica_set)
        node_y = choice(nodes_not_in_replica_set)
        node_z = node_x

        def select_node():
            nonlocal node_z
            node_z = choice(nodes_in_replica_set)
            return node_z != node_x

        wait_until(select_node, timeout_sec=10)

        self.logger.info(f'Move partition from {node_z} to {node_y}')
        part_movement_th = threading.Thread(
            target=self.move_from_src_to_dest,
            args=(node_z, node_y),
        )
        part_movement_th.start()

        # Check that partition movement started/is on-going
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')
        self.logger.info("AWONG finished waiting for in progress")

        self.run_partition_movement_for_n_seconds(self.rp_hostnames,
                                                  timeout_s=1)

        # We could skip the partition movement and just upgrade node_x,
        # however, the point of this test is to check what happens when
        # we upgrade a node while partition movement run concurrently.
        self.restart_node_and_wait_partition_movement(node_id=node_x)
        part_movement_th.join()

    def move_and_upgrade(self, restart_target, move_target):
        admin = Admin(self.redpanda)

        # Start workload
        self.start_workload(runtime=180)

        leader_id = self._wait_for_leader()[0]
        orig_assignments = self._get_assignments(admin, self.topic, 0)
        orig_node_ids = [a["node_id"] for a in orig_assignments]
        brokers = admin.get_brokers()

        # Wait for there to be substantial load.
        wait_until(lambda: self.producer.record_count > 200000,
                   timeout_sec=180,
                   err_msg='Failed to write enough data')

        empty_node_ids = [b["node_id"] for b in brokers if b["node_id"] not in orig_node_ids]

        src_node_id = -1
        if move_target == self.MoveTarget.LEADER:
            src_node_id = leader_id
        elif move_target == self.MoveTarget.FOLLOWER:
            src_node_id = leader_id
            while src_node_id == leader_id:
                src_node_id = choice(orig_node_ids)

        dst_node_id = empty_node_ids[0]
        self.logger.info(f"Moving replica from node {src_node_id} to {dst_node_id}")
        assignments = self._move_replica(self.topic, 0, src_node_id, dst_node_id)

        # Upgrade the leader node to 22.1.3
        node_id_to_upgrade = None
        if restart_target == self.RestartTarget.LEADER:
            node_id_to_upgrade = leader_id
        elif restart_target == self.RestartTarget.MOVE_DEST:
            node_id_to_upgrade = dst_node_id

        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')
        self.logger.info(f"Restarting node {node_id_to_upgrade}")
        node_to_upgrade = self.redpanda.get_node(node_id_to_upgrade)
        self.restart_node_w_version(node_to_upgrade, '22.1.3-1')
        self.confirm_partition_movement_status(self.rp_hostnames,
                                               'in_progress')

        self.admin.wait_stable_configuration(topic=self.topic,
                                             namespace='kafka',
                                             replication=3,
                                             timeout_s=120)

        self.producer.wait(timeout_sec=1200)

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
