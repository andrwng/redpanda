# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.redpanda_installer import RedpandaInstaller


class NodeIdAssignment(RedpandaTest):
    """
    """
    def __init__(self, test_context):
        super(NodeIdAssignment, self).__init__(test_context=test_context,
                                               num_brokers=3)

    def setUp(self):
        self.redpanda.start(auto_assign_node_id=True)
        self._create_initial_topics()

    @cluster(num_nodes=3)
    def test_basic_assignment(self):
        brokers = self.admin.get_brokers()
        self.logger.debug(brokers)
        assert 3 == len(brokers), f"Got {len(brokers)} brokers"

class NodeIdAssignmentUpgrade(RedpandaTest):
    """
    """
    def __init__(self, test_context):
        super(NodeIdAssignmentUpgrade, self).__init__(test_context=test_context,
                                               num_brokers=3)
        self.installer = self.redpanda._installer
        self.admin = self.redpanda._admin

    def setUp(self):
        self.installer.install(self.redpanda.nodes, (22, 2, 1))
        super(NodeIdAssignmentUpgrade, self).setUp()

    @cluster(num_nodes=3)
    def test_assign_after_upgrade(self):
        self.installer.install(self.redpanda.nodes, RedpandaInstaller.HEAD)
        self.redpanda.restart_nodes(self.redpanda.nodes, auto_assign_node_id=True)
        wait_until(
            lambda: self.admin.supports_feature("node_id_assignment"),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timeout waiting for cluster to support 'license' feature")

        clean_node = self.redpanda.nodes[-1]
        original_node_id = self.redpanda.node_id(clean_node)
        self.redpanda.stop_node(clean_node)
        self.redpanda.clean_node(clean_node, preserve_logs=True, preserve_current_install=True)
        self.redpanda.start_node(clean_node, auto_assign_node_id=True)

        brokers = self.admin.get_brokers()
        self.logger.warn(brokers)
        assert 4 == len(brokers), f"Got {len(brokers)} brokers"
        new_node_id = self.redpanda.node_id(clean_node)
        assert original_node_id != new_node_id, f"Cleaned node came back with node ID {new_node_id}"
