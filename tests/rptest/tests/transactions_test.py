# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.tests.upgrade_with_workload import MixedVersionWorkloadRunner
from rptest.util import wait_until_result

import confluent_kafka as ck


class TransactionsTestBase(RedpandaTest):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context, extra_rp_conf=None):
        if not extra_rp_conf:
            extra_rp_conf = {
                "enable_idempotence": True,
                "enable_transactions": True,
                "transaction_coordinator_replication": 3,
                "id_allocator_replication": 3,
                "enable_leader_balancer": False,
                "partition_autobalancing_mode": "off",
            }

        super(TransactionsTestBase, self).__init__(test_context=test_context,
                                                   extra_rp_conf=extra_rp_conf)

        self.input_t = self.topics[0]
        self.output_t = self.topics[1]
        self.max_records = 100

    def on_delivery(self, err, msg):
        assert err == None, msg

    def generate_data(self, topic, num_records):
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
        })

        for i in range(num_records):
            producer.produce(topic.name,
                             str(i),
                             str(i),
                             on_delivery=self.on_delivery)

        producer.flush()

    def consume(self, consumer, max_records=10, timeout_s=2):
        def consume_records():
            records = consumer.consume(max_records, timeout_s)

            if (records != None) and (len(records) != 0):
                return True, records
            else:
                False

        return wait_until_result(consume_records,
                                 timeout_sec=30,
                                 backoff_sec=2,
                                 err_msg="Can not consume data")


class TransactionsTest(TransactionsTestBase):
    topics = (TopicSpec(partition_count=1, replication_factor=3),
              TopicSpec(partition_count=1, replication_factor=3))

    def __init__(self, test_context):
        super(TransactionsTest, self).__init__(test_context)

    @cluster(num_nodes=3)
    def simple_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "test",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])

        num_consumed_records = 0
        while num_consumed_records != self.max_records:
            # Imagine that consume got broken, we read the same record twice and overshoot the condition
            assert num_consumed_records < self.max_records

            records = self.consume(consumer1)

            producer.begin_transaction()

            for record in records:
                assert (record.error() == None)
                producer.produce(self.output_t.name,
                                 record.value(),
                                 record.key(),
                                 on_delivery=self.on_delivery)

            producer.send_offsets_to_transaction(
                consumer1.position(consumer1.assignment()),
                consumer1.consumer_group_metadata())

            producer.commit_transaction()

            num_consumed_records += len(records)

        producer.flush()
        consumer1.close()

        consumer2 = ck.Consumer({
            'group.id': "testtest",
            'bootstrap.servers': self.redpanda.brokers(),
            'auto.offset.reset': 'earliest',
        })
        consumer2.subscribe([self.output_t])

        final_consume_cnt = 0
        expected = bytes("0", 'UTF-8')

        while final_consume_cnt != self.max_records:
            records = self.consume(consumer2)

            for record in records:
                assert (record.key() == expected)
                assert (record.value() == expected)
                expected = bytes(str(int(record.key()) + 1), 'UTF-8')
            final_consume_cnt += len(records)

    @cluster(num_nodes=3)
    def rejoin_member_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        group_name = "test"
        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key())

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer2.subscribe([self.input_t])
        # Rejoin can take some time, so we should pass big timeout
        self.consume(consumer2, timeout_s=360)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
            assert False, "send_offsetes should fail"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError.UNKNOWN_MEMBER_ID

        producer.abort_transaction()

    @cluster(num_nodes=3)
    def change_static_member_test(self):
        self.generate_data(self.input_t, self.max_records)

        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': '0',
            'transaction.timeout.ms': 10000,
        })

        group_name = "test"
        static_group_id = "123"
        consumer1 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'group.instance.id': static_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        producer.init_transactions()

        consumer1.subscribe([self.input_t])
        records = self.consume(consumer1)

        producer.begin_transaction()

        for record in records:
            assert (record.error() == None)
            producer.produce(self.output_t.name, record.value(), record.key())

        offsets = consumer1.position(consumer1.assignment())
        metadata = consumer1.consumer_group_metadata()

        consumer2 = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': group_name,
            'group.instance.id': static_group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })

        consumer2.subscribe([self.input_t])
        self.consume(consumer2)

        try:
            producer.send_offsets_to_transaction(offsets, metadata, 2)
            assert False, "send_offsetes should fail"
        except ck.cimpl.KafkaException as e:
            kafka_error = e.args[0]
            assert kafka_error.code() == ck.cimpl.KafkaError.FENCED_INSTANCE_ID

        producer.abort_transaction()


class MixedVersionTransactionsTest(TransactionsTestBase):
    def __init__(self, test_context):
        extra_rp_conf = {
            "default_topic_replications": 3,
            "enable_auto_rebalance_on_node_add": False,
            "enable_idempotence": True,
            "enable_leader_balancer": False,
            "enable_transactions": True,
            "id_allocator_replication": 3,
            "group_topic_partitions": 1,
            "transaction_coordinator_replication": 3,
        }
        super(MixedVersionTransactionsTest,
              self).__init__(test_context=test_context,
                             extra_rp_conf=extra_rp_conf)
        self.initial_version = MixedVersionWorkloadRunner.PRE_SERDE_VERSION
        self.txn_id = 0

    def setUp(self):
        self.redpanda._installer.install(self.redpanda.nodes,
                                         self.initial_version)
        super(MixedVersionTransactionsTest, self).setUp()

    def txn_workload(self, src_node, dst_node):
        src_id = self.redpanda.idx(src_node)
        dst_id = self.redpanda.idx(dst_node)
        admin = self.redpanda._admin
        # Have the transaction coordinator start out on 'src_node'.
        admin.transfer_leadership_to(namespace="kafka_internal",
                                     topic="tx",
                                     partition=0,
                                     target_id=src_id)
        # The receivers of transaction coordinator RPCs (the leader of the
        # topic being written to and the group topic) should be on 'dst_node'.
        admin.transfer_leadership_to(namespace="kafka",
                                     topic="__consumer_offsets",
                                     partition=0,
                                     target_id=dst_id)
        admin.transfer_leadership_to(namespace="kafka",
                                     topic=self.output_t.name,
                                     partition=0,
                                     target_id=dst_id)

        def run_txn(should_commit):
            self.generate_data(self.output_t, self.max_records)
            producer = ck.Producer({
                'bootstrap.servers': self.redpanda.brokers(),
                'transactional.id': str(self.txn_id),
            })
            producer.init_transactions()
            consumer = ck.Consumer({
                'bootstrap.servers': self.redpanda.brokers(),
                'group.id': "test",
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            })

            consumer.subscribe([self.output_t])
            num_consumed_records = 0

            producer.begin_transaction()
            while num_consumed_records < self.max_records:
                records = self.consume(consumer)
                for r in records:
                    producer.produce(self.output_t.name, r.value(), r.key())
                num_consumed_records += len(records)

            producer.send_offsets_to_transaction(
                consumer.position(consumer.assignment()),
                consumer.consumer_group_metadata())

            if should_commit:
                producer.commit_transaction()
            else:
                producer.abort_transaction()
            producer.flush()
            consumer.close()

            self.txn_id += 1

        run_txn(should_commit=True)
        run_txn(should_commit=False)

    @cluster(num_nodes=3)
    def test_txn_rpcs_with_upgrade(self):
        # Initialize a transaction to bootstrap our transaction internal topic.
        producer = ck.Producer({
            'bootstrap.servers': self.redpanda.brokers(),
            'transactional.id': str(self.txn_id),
        })
        producer.init_transactions()
        self.generate_data(self.output_t, self.max_records)
        self.txn_id += 1
        consumer = ck.Consumer({
            'bootstrap.servers': self.redpanda.brokers(),
            'group.id': "dummy",
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        })
        consumer.subscribe([self.output_t])
        self.consume(consumer)

        MixedVersionWorkloadRunner.upgrade_with_workload(
            self.redpanda, MixedVersionWorkloadRunner.PRE_SERDE_VERSION,
            self.txn_workload)
