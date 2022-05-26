# Copyright 2021 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import re
import threading
from ducktape.services.background_thread import BackgroundThreadService

from rptest.clients.kafka_cli_tools import KafkaCliTools
from ducktape.utils.util import wait_until


class KafkaCliProducer(BackgroundThreadService):
    def __init__(self,
                 context,
                 redpanda,
                 topic,
                 msg_size_mb: int,
                 runtime: int,
                 compression: bool = False,
                 producer_properties={},
                 kafka_version: str = None):
        super(KafkaCliProducer, self).__init__(context, num_nodes=1)
        self._redpanda = redpanda
        self._topic = topic
        self._msg_size_mb = msg_size_mb
        self._runtime = runtime
        self._compression = compression
        self._producer_properties = producer_properties
        self._stopping = threading.Event()

        self._cli = KafkaCliTools(self._redpanda, version=kafka_version)

    def script(self):
        return self._cli._script('kafka-console-producer.sh')

    def _worker(self, _, node):
        self._stopping.clear()
        try:

            # Message size is in MB
            payload = f'dd if=/dev/urandom bs={self._msg_size_mb}M count=1'

            cmd = [f'{payload} | ', self.script()]
            cmd += ["--topic", self._topic]
            if self._compression is not None:
                cmd += ["--compression-codec"]
            for k, v in self._producer_properties.items():
                cmd += ['--producer-property', f"{k}={v}"]

            cmd += ["--broker-list", self._redpanda.brokers()]

            cmd = ' '.join(cmd)

            # Produce N MBs per second in a bash loop
            cmd = f'for (( i=0; i < {self._runtime}; i++ )) ; do {cmd} ; sleep 1 ; done'

            for line in node.account.ssh_capture(cmd):
                line.strip()
                self.logger.debug(line)

                if self._stopping.is_set():
                    break

        except:
            if self._stopping.is_set():
                # Expect a non-zero exit code when killing during teardown
                pass
            else:
                raise
        finally:
            self.done = True

    def stop_node(self, node):
        self._stopping.set()
        node.account.kill_process("java", clean_shutdown=False)
