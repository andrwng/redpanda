# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0


from ducktape.services.service import Service
from ducktape.utils.util import wait_until


class PostgresService(Service):
    """PostgreSQL service for ducktape tests.

    Starts a PostgreSQL instance with logical replication enabled
    (for Debezium CDC). Provides methods to create databases,
    users, tables, and execute SQL.
    """

    PERSISTENT_ROOT = "/var/lib/postgresql-test"
    LOG_FILE = "/var/log/postgresql-test.log"
    PG_PORT = 5432
    DB_NAME = "testdb"
    DB_USER = "dbz"
    DB_PASSWORD = "dbz"

    def __init__(self, ctx, num_nodes=1):
        super().__init__(ctx, num_nodes=num_nodes)

    def _pg_bin(self, node):
        """Find the PostgreSQL binary directory."""
        result = (
            node.account.ssh_output(
                "ls -d /usr/lib/postgresql/*/bin | head -1", allow_fail=True
            )
            .decode()
            .strip()
        )
        return result if result else "/usr/lib/postgresql/16/bin"

    def start_node(self, node, timeout_sec=60):
        pg_bin = self._pg_bin(node)

        # Initialize a fresh data directory
        node.account.ssh(f"mkdir -p {self.PERSISTENT_ROOT}")
        node.account.ssh(
            f"chown -R postgres:postgres {self.PERSISTENT_ROOT}", allow_fail=True
        )
        node.account.ssh(
            f"sudo -u postgres {pg_bin}/initdb -D {self.PERSISTENT_ROOT}/data"
        )

        # Configure for logical replication
        conf = f"{self.PERSISTENT_ROOT}/data/postgresql.conf"
        node.account.ssh(f"echo 'wal_level = logical' >> {conf}")
        node.account.ssh(f"echo 'max_replication_slots = 10' >> {conf}")
        node.account.ssh(f"echo 'max_wal_senders = 10' >> {conf}")
        node.account.ssh(f"echo \"listen_addresses = '*'\" >> {conf}")
        node.account.ssh(f"echo 'port = {self.PG_PORT}' >> {conf}")

        # Allow connections from any host
        hba = f"{self.PERSISTENT_ROOT}/data/pg_hba.conf"
        node.account.ssh(f"echo 'host all all 0.0.0.0/0 md5' >> {hba}")

        # Start PostgreSQL
        node.account.ssh(
            f"sudo -u postgres {pg_bin}/pg_ctl "
            f"-D {self.PERSISTENT_ROOT}/data "
            f"-l {self.LOG_FILE} start"
        )

        # Wait for ready
        wait_until(
            lambda: self._is_ready(node),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg="PostgreSQL did not start in time",
        )

        # Create test user and database
        self._exec_sql_as_postgres(
            node,
            f"CREATE ROLE {self.DB_USER} WITH LOGIN PASSWORD "
            f"'{self.DB_PASSWORD}' REPLICATION",
        )
        self._exec_sql_as_postgres(
            node, f"CREATE DATABASE {self.DB_NAME} OWNER {self.DB_USER}"
        )

    def stop_node(self, node):
        pg_bin = self._pg_bin(node)
        node.account.ssh(
            f"sudo -u postgres {pg_bin}/pg_ctl "
            f"-D {self.PERSISTENT_ROOT}/data stop -m fast",
            allow_fail=True,
        )

    def clean_node(self, node):
        self.stop_node(node)
        node.account.ssh(f"rm -rf {self.PERSISTENT_ROOT}", allow_fail=True)
        node.account.ssh(f"rm -f {self.LOG_FILE}", allow_fail=True)

    def _is_ready(self, node):
        try:
            pg_bin = self._pg_bin(node)
            result = node.account.ssh_output(
                f"sudo -u postgres {pg_bin}/pg_isready -p {self.PG_PORT}",
                allow_fail=True,
            ).decode()
            return "accepting connections" in result
        except Exception:
            return False

    def _exec_sql_as_postgres(self, node, sql):
        node.account.ssh(f'sudo -u postgres psql -p {self.PG_PORT} -c "{sql}"')

    def exec_sql(self, node=None, sql="", database=None):
        """Execute SQL as the test user."""
        node = node or self.nodes[0]
        db = database or self.DB_NAME
        node.account.ssh(
            f"PGPASSWORD={self.DB_PASSWORD} psql "
            f"-h localhost -p {self.PG_PORT} "
            f"-U {self.DB_USER} -d {db} "
            f'-c "{sql}"'
        )

    def exec_sql_output(self, node=None, sql="", database=None):
        """Execute SQL and return the output."""
        node = node or self.nodes[0]
        db = database or self.DB_NAME
        return (
            node.account.ssh_output(
                f"PGPASSWORD={self.DB_PASSWORD} psql "
                f"-h localhost -p {self.PG_PORT} "
                f"-U {self.DB_USER} -d {db} -t -A "
                f'-c "{sql}"'
            )
            .decode()
            .strip()
        )

    def hostname(self, node=None):
        """Return the hostname for external connections."""
        node = node or self.nodes[0]
        return node.account.hostname

    def connection_string(self, node=None):
        """Return a connection string for the test database."""
        return (
            f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}"
            f"@{self.hostname(node)}:{self.PG_PORT}/{self.DB_NAME}"
        )
