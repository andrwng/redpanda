# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import re
import string
from packaging.version import Version

from ducktape.mark import parametrize
from ducktape.utils.util import wait_until
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.tests.end_to_end import EndToEndTest
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.upgrade_with_workload import MixedVersionWorkloadRunner
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST
from rptest.services.redpanda_installer import InstallOptions, RedpandaInstaller, wait_for_num_versions

from confluent_kafka.admin import AdminClient, ConfigResource, RESOURCE_TOPIC


class UpgradeFromSpecificVersion(RedpandaTest):
    """
    Basic test that upgrading software works as expected.
    """
    def __init__(self, test_context):
        super(UpgradeFromSpecificVersion,
              self).__init__(test_context=test_context, num_brokers=3)
        self.installer = self.redpanda._installer

    def setUp(self):
        # NOTE: `rpk redpanda admin brokers list` requires versions v22.1.x and
        # above.
        self.installer.install(self.redpanda.nodes, (22, 1, 3))
        super(UpgradeFromSpecificVersion, self).setUp()

    @cluster(num_nodes=3, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_basic_upgrade(self):
        first_node = self.redpanda.nodes[0]

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" in unique_versions, unique_versions

        # Upgrade one node to the head version.
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 2)
        assert "v22.1.3" in unique_versions, unique_versions

        # Rollback the partial upgrade and ensure we go back to the original
        # state.
        self.installer.install([first_node], (22, 1, 3))
        self.redpanda.restart_nodes([first_node])
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" in unique_versions, unique_versions

        # Only once we upgrade the rest of the nodes do we converge on the new
        # version.
        self.installer.install([first_node], RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes)
        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert "v22.1.3" not in unique_versions, unique_versions


class UpgradeFromPriorFeatureVersionTest(RedpandaTest):
    """
    Basic test that installs the previous feature version and performs an
    upgrade.
    """
    def __init__(self, test_context):
        super(UpgradeFromPriorFeatureVersionTest,
              self).__init__(test_context=test_context, num_brokers=1)
        self.installer = self.redpanda._installer

    def setUp(self):
        self.prev_version = \
            self.installer.highest_from_prior_feature_version(RedpandaInstaller.HEAD)
        self.installer.install(self.redpanda.nodes, self.prev_version)
        super(UpgradeFromPriorFeatureVersionTest, self).setUp()

    @cluster(num_nodes=1,
             log_allow_list=RESTART_LOG_ALLOW_LIST +
             [re.compile("cluster - .*Error while reconciling topic.*")])
    def test_basic_upgrade(self):
        node = self.redpanda.nodes[0]
        initial_version = Version(self.redpanda.get_version(node))
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        self.redpanda.restart_nodes([node])
        head_version_str = self.redpanda.get_version(node)
        head_version = Version(head_version_str)
        assert initial_version < head_version, f"{initial_version} vs {head_version}"

        unique_versions = wait_for_num_versions(self.redpanda, 1)
        assert head_version_str in unique_versions, unique_versions


class UpgradeWithWorkloadTest(EndToEndTest):
    """
    Test class that performs upgrades while verifying a concurrently running
    workload is making progress.
    """
    def setUp(self):
        super(UpgradeWithWorkloadTest, self).setUp()
        # Start at a version that supports rolling restarts.
        self.initial_version = (22, 1, 3)
        self.producer_msgs_per_sec = 1
        install_opts = InstallOptions(version=self.initial_version)
        self.start_redpanda(num_nodes=3, install_opts=install_opts)
        self.installer = self.redpanda._installer

        # Start running a workload.
        spec = TopicSpec(name="topic", partition_count=2, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(num_nodes=1, throughput=self.producer_msgs_per_sec)
        self.start_consumer(num_nodes=1)
        self.await_startup(min_records=self.producer_msgs_per_sec)

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_rolling_upgrade(self):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                            start_timeout=90,
                                            stop_timeout=90)

        post_upgrade_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_upgrade_num_msgs +
                            (self.producer_msgs_per_sec * 3))

    @cluster(num_nodes=5, log_allow_list=RESTART_LOG_ALLOW_LIST)
    @parametrize(upgrade_after_rollback=True)
    @parametrize(upgrade_after_rollback=False)
    def test_rolling_upgrade_with_rollback(self, upgrade_after_rollback):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)

        # Upgrade one node.
        first_node = self.redpanda.nodes[0]
        # Give ample time to restart, given the running workload.
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)

        def await_progress():
            num_msgs = self.producer.num_acked
            self.await_num_produced(num_msgs +
                                    (self.producer_msgs_per_sec * 3))
            self.await_num_consumed(num_msgs +
                                    (self.producer_msgs_per_sec * 3))

        # Ensure that after we upgrade a node, we're still able to make
        # progress.
        await_progress()

        # Then roll it back; we should still be able to make progress.
        self.installer.install([first_node], self.initial_version)
        self.redpanda.rolling_restart_nodes([first_node],
                                            start_timeout=90,
                                            stop_timeout=90)
        await_progress()

        if upgrade_after_rollback:
            self.installer.install([first_node], RedpandaInstaller.HEAD)
            self.redpanda.rolling_restart_nodes(self.redpanda.nodes,
                                                start_timeout=90,
                                                stop_timeout=90)

        post_rollback_num_msgs = self.producer.num_acked
        self.run_validation(min_records=post_rollback_num_msgs +
                            (self.producer_msgs_per_sec * 3),
                            enable_idempotence=True)


class RaftRpcUpgradeTest(EndToEndTest):
    """
    This test generates Raft election traffic back and forth between two nodes,
    all the while being upgraded.
    """
    def setUp(self):
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION
        self.producer_msgs_per_sec = 1
        install_opts = InstallOptions(version=self.initial_version)
        extra_rp_conf = dict(enable_leader_balancer=False)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            extra_rp_conf=extra_rp_conf)

        # Start running a workload.
        spec = TopicSpec(name="topic", partition_count=2, replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name
        self.start_producer(num_nodes=1, throughput=self.producer_msgs_per_sec)
        self.start_consumer(num_nodes=1)
        self.await_startup(min_records=self.producer_msgs_per_sec)

    def verify_leadership_transfer_to(self, dst_node):
        def await_progress():
            num_msgs = self.producer.num_acked
            self.await_num_produced(num_msgs +
                                    (self.producer_msgs_per_sec * 3))
            self.await_num_consumed(num_msgs +
                                    (self.producer_msgs_per_sec * 3))

        await_progress()
        admin = self.redpanda._admin
        dst_id = self.redpanda.idx(dst_node)
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=self.topic,
                                     partition=0,
                                     target_id=dst_id)

        admin.await_stable_leader(topic=self.topic,
                                  check=lambda node_id: node_id == dst_id)
        await_progress()

    def raft_workload(self, src_node, dst_node):
        # Send leadership to 'src_node' first to exercise leadership transfer
        # RPCs that get sent when transferring leadership to 'dst_node' too.
        self.verify_leadership_transfer_to(src_node)
        self.verify_leadership_transfer_to(dst_node)

    @cluster(num_nodes=5,
             log_allow_list=MixedVersionWorkloadRunner.ALLOWED_LOGS)
    def test_raft_traffic_during_upgrade(self):
        MixedVersionWorkloadRunner.upgrade_with_workload(
            self.redpanda, self.initial_version, self.raft_workload)


def rand_with_prefix(prefix):
    return f"{prefix}-{''.join([random.choice(string.ascii_letters) for _ in range(5)])}"


class KafkaRpcUpgradeTest(EndToEndTest):
    """
    This test uses admin operations to exercise Kafka admin RPC traffic between
    two nodes, all the while being upgraded.
    """
    def setUp(self):
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION
        install_opts = InstallOptions(version=self.initial_version)
        extra_rp_conf = dict(enable_leader_balancer=False,
                             auto_create_topics_enabled=True)
        self.start_redpanda(num_nodes=3,
                            install_opts=install_opts,
                            extra_rp_conf=extra_rp_conf)

    @cluster(num_nodes=3)
    def test_create_topics(self):
        self.created_topics = []

        def create_and_update_random_topic(src_node, dst_node):
            admin = self.redpanda._admin
            dst_id = self.redpanda.idx(dst_node)
            # Move controller leadership to 'dst_node'. The client will send a
            # request to 'src_node' and Redpanda to forward a request to
            # 'dst_node'.
            admin.transfer_leadership_to(namespace="redpanda",
                                         topic="controller",
                                         partition=0,
                                         target_id=dst_id)
            admin.await_stable_leader(namespace="redpanda",
                                      topic="controller",
                                      partition=0)
            topic_name = rand_with_prefix("topic")
            # Leave only 'src_node' in the bootstrap servers to encourage the
            # initial request to go to it.
            ac1 = AdminClient(
                {"bootstrap.servers": self.redpanda.broker_address(src_node)})
            ac1.list_topics(topic_name)
            self.created_topics.append(topic_name)

            # Create a new client to reset its metadata.
            ac2 = AdminClient(
                {"bootstrap.servers": self.redpanda.broker_address(src_node)})
            resources = [
                ConfigResource(RESOURCE_TOPIC,
                               topic_name,
                               set_config={"retention.ms": "1000"})
            ]
            ac2.alter_configs(resources)

        MixedVersionWorkloadRunner.upgrade_with_workload(
            self.redpanda, self.initial_version,
            create_and_update_random_topic)

        ck_admin = AdminClient({"bootstrap.servers": self.redpanda.brokers()})
        num_topics = len(ck_admin.list_topics().topics)
        num_expected = len(self.created_topics)
        assert num_topics == num_expected, f"Expected {num_expected} topics, got {num_topics}"


class AdminRpcUpgradeTest(EndToEndTest):
    def setUp(self):
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION
        install_opts = InstallOptions(version=self.initial_version)
        # This test is sensitive to leadership changes, so make sure we only
        # change leaders intentionally.
        extra_rp_conf = dict(enable_leader_balancer=False)
        self.start_redpanda(num_nodes=4,
                            install_opts=install_opts,
                            extra_rp_conf=extra_rp_conf)

    def run_admin_rpcs(self, src_node, dst_node):
        """
        Performs several admin operations. This exercises requests sent by
        the controller, as the RPCs are expected to land at the controller
        leader 'dst_node' and they must be forwarded from 'src_node'.
        """
        # Create new topic partitions to help ensure node decommissioning takes
        # time and we can slip in a recommission.
        spec = TopicSpec(name=rand_with_prefix("topic"),
                         partition_count=2,
                         replication_factor=3)
        self.client().create_topic(spec)

        admin = self.redpanda._admin
        dst_id = self.redpanda.idx(dst_node)
        wait_until(lambda: admin.transfer_leadership_to(namespace="redpanda",
                                                        topic="controller",
                                                        partition=0,
                                                        target_id=dst_id),
                   timeout_sec=60,
                   backoff_sec=1)
        admin.await_stable_leader(namespace="redpanda",
                                  topic="controller",
                                  partition=0)
        # Avoid nodes of interest by operating on the back node.
        back_node = self.redpanda.nodes[-1]
        back_id = self.redpanda.idx(back_node)

        # Send all our admin requests to src node.
        src_admin = Admin(self.redpanda, default_node=src_node)

        # Throttle partition movement so we can slip in a recommission before
        # the node is fully decommissioned.
        rpk = RpkTool(self.redpanda)
        rpk.cluster_config_set("raft_learner_recovery_rate", "1")

        def check_decommissioning(expect_decommissioning: bool):
            try:
                brokers = admin.get_brokers()
                for b in brokers:
                    if not b["node_id"] == back_id:
                        continue
                    is_decommissioning = b["membership_status"] == "draining"
                    if expect_decommissioning:
                        return is_decommissioning
                    else:
                        return not is_decommissioning
            except:
                return False
            # If 'back_node' is not in the brokers, we've decommissioned.
            return expect_decommissioning

        src_admin.decommission_broker(back_id)
        wait_until(lambda: check_decommissioning(True),
                   timeout_sec=60,
                   backoff_sec=1)
        src_admin.recommission_broker(back_id)
        wait_until(lambda: check_decommissioning(False),
                   timeout_sec=60,
                   backoff_sec=1)

        # Revert the recovery rate to proceed without trouble.
        rpk.cluster_config_set("raft_learner_recovery_rate", "104857600")

        def check_maintenance(expect_maintenance: bool):
            try:
                status = src_admin.maintenance_status(back_node)
                in_maintenance = status["draining"]
                if expect_maintenance:
                    return in_maintenance
                else:
                    return not in_maintenance
            except:
                return False

        src_admin.maintenance_start(back_node)
        wait_until(lambda: check_maintenance(True),
                   timeout_sec=60,
                   backoff_sec=1)
        src_admin.maintenance_stop(back_node)
        wait_until(lambda: check_maintenance(False),
                   timeout_sec=60,
                   backoff_sec=1)

        # Get the health monitor status, which dispatches to the leader.
        _ = src_admin.get_cluster_view(src_node)

        def change_config_and_wait(new_config_value):
            cluster_config_upsert = dict(
                {'log_message_timestamp_type': new_config_value})
            patch_result = src_admin.patch_cluster_config(
                upsert=cluster_config_upsert, node=src_node)
            new_version = patch_result["config_version"]
            wait_until(
                lambda: set([
                    n['config_version']
                    for n in src_admin.get_cluster_config_status()
                ]) == {new_version},
                timeout_sec=10,
                backoff_sec=0.5,
                err_msg=
                f"Config status versions did not converge on {new_version}")

        # Change the value a couple times so we're guaranteed that repeated
        # runs of this function actually change the value.
        change_config_and_wait("CreateTime")
        change_config_and_wait("LogAppendTime")

    @cluster(num_nodes=6,
             log_allow_list=MixedVersionWorkloadRunner.ALLOWED_LOGS)
    def test_admin_rpcs(self):
        spec = TopicSpec(name=rand_with_prefix("topic"),
                         partition_count=2,
                         replication_factor=3)
        self.client().create_topic(spec)
        self.topic = spec.name

        # Start a workload just so replica movement isn't immediate when
        # decommissioning, allowing us time to recommission.
        self.start_producer(1, throughput=100)
        self.start_consumer(1)
        self.await_startup()

        # This workload creates a few topics that may slow down cluster
        # lifecycle. Extend the timeouts.
        MixedVersionWorkloadRunner.upgrade_with_workload(self.redpanda,
                                                         self.initial_version,
                                                         self.run_admin_rpcs,
                                                         start_timeout=90,
                                                         stop_timeout=90)
