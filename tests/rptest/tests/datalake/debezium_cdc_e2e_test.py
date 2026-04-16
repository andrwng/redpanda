# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""End-to-end tests: PostgreSQL -> Debezium -> Redpanda -> Iceberg

Verifies that Debezium CDC events (inserts, updates, deletes) are
correctly translated to Iceberg tables with proper upsert and delete
semantics across different partition specs and key configurations.
"""

import time

from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.debezium_server_service import DebeziumServerService
from rptest.services.postgres_service import PostgresService
from rptest.services.redpanda import SISettings, SchemaRegistryConfig
from rptest.tests.datalake.datalake_services import DatalakeServices
from rptest.tests.datalake.query_engine_base import QueryEngineType
from rptest.tests.datalake.utils import supported_storage_types
from rptest.tests.redpanda_test import RedpandaTest


class DebeziumCdcIcebergTest(RedpandaTest):
    """End-to-end tests: PostgreSQL -> Debezium -> Redpanda -> Iceberg"""

    def __init__(self, test_ctx):
        super().__init__(
            test_ctx,
            num_brokers=1,
            si_settings=SISettings(test_context=test_ctx),
            schema_registry_config=SchemaRegistryConfig(),
            extra_rp_conf={
                "iceberg_enabled": True,
                "iceberg_catalog_commit_interval_ms": 5000,
            },
        )
        self.postgres = PostgresService(test_ctx)
        self.debezium = None
        self.dl = DatalakeServices(
            self.test_context,
            redpanda=self.redpanda,
            include_query_engines=[QueryEngineType.SPARK],
        )

    def setUp(self):
        self.postgres.start()
        self.dl.setUp()

    def tearDown(self):
        if self.debezium:
            self.debezium.stop()
        self.dl.tearDown()
        self.postgres.stop()
        super().tearDown()

    def _start_debezium(self, table_name, server_name="dbserver1"):
        """Start Debezium Server capturing a specific table."""
        self.debezium = DebeziumServerService(
            self.test_context,
            self.redpanda,
            self.postgres,
            database_name=PostgresService.DB_NAME,
            table_include_list=f"public.{table_name}",
            server_name=server_name,
        )
        self.debezium.start()
        return self.debezium.topic_name(table=table_name)

    def _spark_query(self, topic_name, query_suffix):
        """Build a Spark SQL query against an Iceberg table."""
        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(topic_name)}"
        return spark, f"{query_suffix.replace('$TBL', tbl)}"

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_debezium_cdc_to_iceberg(self, cloud_storage_type):
        """Basic CDC: insert, update, delete with identity(id) partition."""
        topic = "dbserver1.public.users"
        self.dl.create_iceberg_enabled_topic(
            topic,
            iceberg_mode="debezium_schema_id_prefix",
            config={
                TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC: "(identity(id))",
            },
        )

        self.postgres.exec_sql(
            sql="CREATE TABLE users ("
            "id SERIAL PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "email TEXT"
            ")"
        )
        self.postgres.exec_sql(sql="ALTER TABLE users REPLICA IDENTITY FULL")
        self.postgres.exec_sql(
            sql="INSERT INTO users (name, email) VALUES "
            "('alice', 'alice@example.com'), "
            "('bob', 'bob@example.com'), "
            "('charlie', 'charlie@example.com')"
        )

        self._start_debezium("users")
        self.dl.wait_for_translation(topic, msg_count=3)

        self.postgres.exec_sql(sql="UPDATE users SET name = 'alicia' WHERE id = 1")
        self.postgres.exec_sql(sql="DELETE FROM users WHERE id = 2")

        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(topic)}"

        def _check():
            try:
                rows = spark.run_query_fetch_all(
                    f"SELECT id, name, email FROM {tbl} ORDER BY id"
                )
                self.logger.info(f"Rows: {rows}")
                return rows == [
                    (1, "alicia", "alice@example.com"),
                    (3, "charlie", "charlie@example.com"),
                ]
            except Exception as e:
                self.logger.info(f"Query failed: {e}")
                return False

        wait_until(
            _check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CDC final state not reflected in Iceberg",
        )

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_debezium_no_partition(self, cloud_storage_type):
        """CDC with an unpartitioned table. Equality deletes apply
        globally when the partition spec is empty."""
        topic = "dbserver1.public.items"
        self.dl.create_iceberg_enabled_topic(
            topic,
            iceberg_mode="debezium_schema_id_prefix",
        )
        # Override the default partition spec to be unpartitioned.
        from rptest.clients.rpk import RpkTool

        rpk = RpkTool(self.redpanda)
        rpk.alter_topic_config(topic, TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC, "()")

        self.postgres.exec_sql(
            sql="CREATE TABLE items (id SERIAL PRIMARY KEY, label TEXT NOT NULL)"
        )
        self.postgres.exec_sql(sql="ALTER TABLE items REPLICA IDENTITY FULL")
        self.postgres.exec_sql(
            sql="INSERT INTO items (label) VALUES ('x'), ('y'), ('z')"
        )

        self._start_debezium("items")
        self.dl.wait_for_translation(topic, msg_count=3)

        self.postgres.exec_sql(sql="UPDATE items SET label = 'X' WHERE id = 1")
        self.postgres.exec_sql(sql="DELETE FROM items WHERE id = 3")

        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(topic)}"

        def _check():
            try:
                rows = spark.run_query_fetch_all(
                    f"SELECT id, label FROM {tbl} ORDER BY id"
                )
                self.logger.info(f"Rows: {rows}")
                return rows == [(1, "X"), (2, "y")]
            except Exception as e:
                self.logger.info(f"Query failed: {e}")
                return False

        wait_until(
            _check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CDC final state not reflected in Iceberg",
        )

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_debezium_composite_key(self, cloud_storage_type):
        """CDC with a composite primary key that is a superset of the
        partition key. PK=(id, region), partition=identity(id)."""
        topic = "dbserver1.public.orders"
        self.dl.create_iceberg_enabled_topic(
            topic,
            iceberg_mode="debezium_schema_id_prefix",
            config={
                TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC: "(identity(id))",
            },
        )

        self.postgres.exec_sql(
            sql="CREATE TABLE orders ("
            "id INT NOT NULL, "
            "region TEXT NOT NULL, "
            "amount INT, "
            "PRIMARY KEY (id, region)"
            ")"
        )
        self.postgres.exec_sql(sql="ALTER TABLE orders REPLICA IDENTITY FULL")
        self.postgres.exec_sql(
            sql="INSERT INTO orders (id, region, amount) VALUES "
            "(1, 'us', 100), (2, 'eu', 200), (3, 'us', 300)"
        )

        self._start_debezium("orders")
        self.dl.wait_for_translation(topic, msg_count=3)

        self.postgres.exec_sql(
            sql="UPDATE orders SET amount = 150 WHERE id = 1 AND region = 'us'"
        )
        self.postgres.exec_sql(sql="DELETE FROM orders WHERE id = 2 AND region = 'eu'")

        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(topic)}"

        def _check():
            try:
                rows = spark.run_query_fetch_all(
                    f"SELECT id, region, amount FROM {tbl} ORDER BY id"
                )
                self.logger.info(f"Rows: {rows}")
                return rows == [(1, "us", 150), (3, "us", 300)]
            except Exception as e:
                self.logger.info(f"Query failed: {e}")
                return False

        wait_until(
            _check,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="CDC final state not reflected in Iceberg",
        )

    @cluster(num_nodes=5)
    @matrix(cloud_storage_type=supported_storage_types())
    def test_debezium_partition_key_validation(self, cloud_storage_type):
        """When the delete key is a strict subset of the partition spec
        source columns, translation of upserts/deletes should fail
        (retriable) because equality deletes cannot be correctly scoped.

        PK=(id), partition=(identity(id), identity(kind)). The delete
        key {id} doesn't include {kind}."""
        topic = "dbserver1.public.events"
        self.dl.create_iceberg_enabled_topic(
            topic,
            iceberg_mode="debezium_schema_id_prefix",
            config={
                TopicSpec.PROPERTY_ICEBERG_PARTITION_SPEC: "(identity(id), identity(kind))",
            },
        )

        self.postgres.exec_sql(
            sql="CREATE TABLE events ("
            "id SERIAL PRIMARY KEY, "
            "kind TEXT NOT NULL, "
            "payload TEXT"
            ")"
        )
        self.postgres.exec_sql(sql="ALTER TABLE events REPLICA IDENTITY FULL")
        self.postgres.exec_sql(
            sql="INSERT INTO events (kind, payload) VALUES ('a', 'data')"
        )

        self._start_debezium("events")

        # Initial insert translates fine (no delete key validation for
        # insert-only records).
        self.dl.wait_for_translation(topic, msg_count=1)

        # An update produces a delete key, triggering validation.
        # Since the key {id} is a subset of partition sources {id, kind},
        # translation should stall.
        self.postgres.exec_sql(sql="UPDATE events SET payload = 'updated' WHERE id = 1")

        # Wait long enough for the update to reach Redpanda and for
        # the translator to attempt (and fail) the batch.
        time.sleep(30)

        spark = self.dl.query_engine(QueryEngineType.SPARK)
        tbl = f"redpanda.{spark.escape_identifier(topic)}"
        rows = spark.run_query_fetch_all(f"SELECT payload FROM {tbl}")
        assert len(rows) == 1 and rows[0][0] == "data", (
            f"Update should not propagate when delete key is a subset "
            f"of partition source columns, but got: {rows}"
        )
