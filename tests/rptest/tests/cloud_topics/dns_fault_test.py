# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Catches a shard that permanently stops reaching object storage.

1. Make DNS look like Kubernetes. Before any broker starts, rewrite
   /etc/resolv.conf on all three nodes.
   a. Keep the existing nameserver line.
   b. Add a three domain search list.
   c. Add options ndots:5.
   d. The S3 hostname has fewer than five dots, so the resolver tries each
      search domain first: three NXDOMAINs, each answer starting the next
      query. That chain is the trigger, because it recycles query ids.
   e. This has to happen before start. c-ares reads the file once, when a
      shard creates its DNS channel.

2. Start three brokers tuned to resolve DNS constantly.
   a. Idle timeout zero, so no connection is reused and every request
      redials and re-resolves.
   b. Upload interval 25ms rather than 250ms.
   c. A four client pool, small enough that one stuck shard pins all of it.

3. Create a cloud topic with 24 partitions, so every shard does work.

4. Produce continuously. Each upload costs one lookup, about 13 per second
   per shard.

5. Poll every 20 seconds for four per-shard metrics: completed requests,
   active requests, pool utilization, transport errors.

6. Fail if one shard shows all of these across two consecutive samples.
   a. Pool utilization at 100%.
   b. Active requests above zero.
   c. Nothing completed between the samples.
   d. Two samples rather than one, because a burst of uploads can saturate
      a healthy pool briefly, but it drains.
   e. Transport errors stay at zero throughout: the pinned requests never
      reach a socket, so nothing fails.

7. After ten minutes, fail if fewer than 5000 requests happened, because
   the defect fires around 111 times per million lookups.

Step 6 firing is the result this test exists to produce: it names a stranded
shard, and aborts the loop early, so a red run of a couple of minutes is the
bug. Step 7 firing is not a result at all. It means the run never generated
the traffic it was supposed to, so it says nothing about the resolver either
way, and points at the load path or the host rather than at c-ares. A
healthy run clears the threshold by an order of magnitude.

The claim: a shard that saturates its cloud storage client pool must drain
it. On c-ares 1.34.7 a shard stops draining within minutes; on 1.34.6 it
does not.

No fault injection anywhere. Ordinary produce traffic is enough.
"""

import time
from dataclasses import dataclass

from ducktape.cluster.cluster import ClusterNode
from ducktape.mark import matrix
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.context.cloud_storage import CloudStorageType
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import (
    MetricsEndpoint,
    SISettings,
    get_cloud_storage_type,
)
from rptest.tests.redpanda_test import RedpandaTest

SEARCH_DOMAINS = ("a.invalid", "b.invalid", "c.invalid")

SOAK_S = 600
SAMPLE_INTERVAL_S = 20
MIN_REQUESTS = 5000

PARTITION_COUNT = 24
PRODUCE_MSG_SIZE = 16384
PRODUCE_RATE_BPS = 12 * 1024 * 1024


@dataclass
class ShardState:
    completed_requests: int = 0
    active_requests: int = 0
    pool_utilization: int = 0
    transport_errors: int = 0

    def pinned(self) -> bool:
        return self.pool_utilization >= 100 and self.active_requests > 0


class CloudTopicsDnsFaultTest(RedpandaTest):
    """A shard that saturates its cloud storage client pool must drain it.

    A DNS lookup that neither completes nor expires holds its shard's resolver
    mutex for the life of the process, so every later connect on that shard
    queues behind it holding a client lease. See c-ares/c-ares#1256.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=SISettings(
                test_context=test_context,
                cloud_storage_max_connections=4,
                cloud_storage_enable_remote_read=False,
                cloud_storage_enable_remote_write=False,
                fast_uploads=True,
            ),
            extra_rp_conf={
                "cloud_storage_max_connection_idle_time_ms": 0,
                "cloud_topics_produce_upload_interval": 25,
                "cloud_topics_reconciliation_min_interval": 2000,
                "cloud_topics_reconciliation_max_interval": 2000,
            },
        )

    def setUp(self) -> None:
        self._use_search_list()
        super().setUp()

    def _use_search_list(self) -> None:
        """Give the resolver a search list, which is the whole trigger.

        The docker test node ships ndots:0, which resolves absolute-first and
        never walks the list, so without this the test passes on every c-ares
        version.
        """
        search = " ".join(SEARCH_DOMAINS)
        for node in self.redpanda.nodes:
            node.account.ssh(
                "cp /etc/resolv.conf /etc/resolv.conf.orig; "
                "{ grep '^nameserver' /etc/resolv.conf.orig; "
                f"echo 'search {search}'; "
                "echo 'options ndots:5'; } > /etc/resolv.conf"
            )
            contents = node.account.ssh_output("cat /etc/resolv.conf").decode(
                "utf-8", errors="replace"
            )
            self.logger.info(f"{node.name} resolv.conf:\n{contents}")

    def _shard_state(self, node: ClusterNode) -> dict[int, ShardState]:
        out: dict[int, ShardState] = {}

        def collect(metric: str, field: str) -> None:
            samples = self.redpanda.metrics_sample(
                name=metric,
                nodes=[node],
                metrics_endpoint=MetricsEndpoint.METRICS,
            )
            if samples is None:
                return
            for sample in samples.samples:
                raw = sample.labels.get("shard")
                if raw is None:
                    continue
                state = out.setdefault(int(raw), ShardState())
                setattr(state, field, getattr(state, field) + int(sample.value))

        collect("vectorized_cloud_client_all_requests", "completed_requests")
        collect("vectorized_cloud_client_active_requests", "active_requests")
        collect("vectorized_cloud_client_client_pool_utilization", "pool_utilization")
        collect("vectorized_cloud_client_num_transport_errors", "transport_errors")
        return out

    def _sample(self) -> dict[tuple[str, int], ShardState]:
        return {
            (node.name, shard): state
            for node in self.redpanda.nodes
            for shard, state in self._shard_state(node).items()
        }

    @cluster(
        num_nodes=4,
        log_allow_list=[
            ".*client_pool - .*Lease expired after.*",
            ".*cloud_storage - .*",
            ".*cloud_topics - .*",
        ],
    )
    @matrix(
        cloud_storage_type=get_cloud_storage_type(applies_only_on=[CloudStorageType.S3])
    )
    def test_no_shard_strands_itself(
        self, cloud_storage_type: CloudStorageType
    ) -> None:
        topic = TopicSpec(partition_count=PARTITION_COUNT, replication_factor=3)
        RpkTool(self.redpanda).create_topic(
            topic.name,
            topic.partition_count,
            topic.replication_factor,
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_CLOUD},
        )

        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            topic.name,
            msg_size=PRODUCE_MSG_SIZE,
            msg_count=10_000_000,
            rate_limit_bps=PRODUCE_RATE_BPS,
            tolerate_failed_produce=True,
        )
        producer.start()
        try:
            deadline = time.monotonic() + SOAK_S
            previous = self._sample()
            requests = 0
            while time.monotonic() < deadline:
                time.sleep(SAMPLE_INTERVAL_S)
                current = self._sample()

                for key, now in current.items():
                    was = previous.get(key)
                    if was is None:
                        continue
                    progress = now.completed_requests - was.completed_requests
                    requests += max(progress, 0)
                    if was.pinned() and now.pinned() and progress == 0:
                        node_name, shard = key
                        raise AssertionError(
                            f"{node_name} shard {shard} stopped reaching "
                            f"object storage: pool fully utilized with "
                            f"{now.active_requests} requests pinned across "
                            f"{SAMPLE_INTERVAL_S}s, nothing completed, and no "
                            f"transport error to show for it "
                            f"(errors: {now.transport_errors}). That is a DNS "
                            "lookup neither completed nor expired holding the "
                            "shard's resolver mutex; see c-ares/c-ares#1256"
                        )
                previous = current

            self.logger.info(
                f"no shard stranded in {SOAK_S}s over roughly {requests} "
                "object storage requests"
            )
            assert requests > MIN_REQUESTS, (
                f"only {requests} object storage requests in {SOAK_S}s, too "
                "few for a ~111 ppm event, so this run cannot tell a healthy "
                "resolver from a lucky one"
            )
        finally:
            producer.stop()
