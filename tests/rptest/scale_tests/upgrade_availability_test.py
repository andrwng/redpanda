from rptest.services.cluster import cluster
from rptest.clients.types import TopicSpec
from ducktape.utils.util import wait_until
from ducktape.mark import ignore
from rptest.services.admin import Admin
import confluent_kafka as ck
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.verifiable_producer import VerifiableProducer, is_int_with_prefix
from rptest.services.kaf_producer import KafProducer
from rptest.services.rpk_consumer import RpkConsumer
import time
from random import choice


class UpgradeAvailabilityTest(RedpandaTest):
    # TODO: See how many topics to use
    topics = (TopicSpec(partition_count=8, replication_factor=3), )

    def __init__(self, test_context):
        super(UpgradeAvailabilityTest,
              self).__init__(test_context=test_context, num_brokers=8)

        self.consumer = None
        self.producer = None

    def setUp(self):

        # Make sure each RP node has the list of updated
        # redanda packages
        for node in self.redpanda.nodes:
            cmd = "curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash"
            node.account.ssh(cmd, allow_fail=False)

        super().setUp()  # RP nodes start in super

    def restart_node_w_version(self, node, version: str):
        self.redpanda.stop_node(node, timeout=300)
        cmd = f'sudo apt -o  Dpkg::Options::="--force-confnew" install redpanda={version}'
        node.account.ssh(cmd, allow_fail=False)
        # node.account.ssh("sudo systemctl stop redpanda")
        self.redpanda.start_node(node, None, timeout=300)

    def restart_nodes_w_version(self, version: str):
        for node in self.redpanda.nodes:
            self.restart_node_w_version(node, version)

    # TODO: See how man producers and consumers to use.
    # Each producer writes 600Mbps and the consumers read until completion.
    def start_workload(self):
        if self.producer is not None:
            raise RuntimeError('Producer is already defined')

        # Need to change the endpoints do kafka 2.8 clients
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            redpanda=self.redpanda,
            topic=self.topic,
            throughput=1000,
            message_validator=is_int_with_prefix)
        self.producer.start()

        wait_until(lambda: self.producer.num_acked > 100,
                   timeout_sec=10,
                   err_msg='Producer failed to produce messages for 10s')

        if self.consumer is not None:
            raise RuntimeError('Consumer is already defined')

        self.consumer = RpkConsumer(context=self.test_context,
                                    redpanda=self.redpanda,
                                    topic=self.topic)
        self.consumer.start()
        time.sleep(10)
        for msg in self.consumer.messages:
            print(f'value: {msg["value"]}, size: {len(msg["value"])}')
        self.consumer.stop()
        self.consumer.wait()

        self.producer.stop()

    @cluster(num_nodes=10)
    def upgrade_availability_test(self):

        # Use v21.11.15-1-7325762b on all nodes
        self.restart_nodes_w_version('21.11.15-1-7325762b')

        # Start workload
        self.start_workload()

        # Upgrade one of the nodes to 22.1.3
        node = choice(self.redpanda.nodes)
        self.restart_node_w_version(node, '22.1.3-1')
