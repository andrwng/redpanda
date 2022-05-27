from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import CHAOS_LOG_ALLOW_LIST
from rptest.services.kafka_cli_consumer import KafkaCliConsumer
from rptest.services.kafka_cli_producer import KafkaCliProducer
import time
from random import choice


class UpgradeAvailabilityTest(RedpandaTest):
    # TODO: See how many topics to use
    topics = (TopicSpec(partition_count=8, replication_factor=3), )

    def __init__(self, test_context):
        super(UpgradeAvailabilityTest,
              self).__init__(test_context=test_context,
                             num_brokers=8,
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
                                         msg_size_mb=100,
                                         runtime=runtime,
                                         compression=True,
                                         kafka_version='2.8.0')
        self.producer.start()

        if self.consumer is not None:
            raise RuntimeError('Consumer is already defined')

        self.consumer = KafkaCliConsumer(self.test_context,
                                         self.redpanda,
                                         self.topic,
                                         from_beginning=True,
                                         kafka_version='2.8.0',
                                         save_msgs=False)
        self.consumer.start()
        time.sleep(1)
        # Make sure consumer is recieving some data
        # self.consumer.wait_for_messages(10)

    def wait_on_workload(self):
        # Terminate when the producer finishes
        self.producer.wait()
        self.consumer.stop()

    @cluster(num_nodes=10, log_allow_list=CHAOS_LOG_ALLOW_LIST)
    def upgrade_availability_test(self):

        # Start workload
        self.start_workload(runtime=30)

        # Upgrade one of the nodes to 22.1.3
        node = choice(self.redpanda.nodes)
        self.restart_node_w_version(node, '22.1.3-1')

        self.wait_on_workload()
