# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import random

from rptest.services.cluster import cluster
from ducktape.utils.util import wait_until
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.http_server import HttpServer
from rptest.services.redpanda import RedpandaService


class MetricsReporterTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        self.http = HttpServer(self._ctx)
        super(MetricsReporterTest, self).__init__(
            test_context=test_ctx,
            extra_rp_conf={
                "health_monitor_tick_interval": 1000,
                # report every two seconds
                "metrics_reporter_tick_interval": 2000,
                "metrics_reporter_report_interval": 1000,
                "enable_metrics_reporter": True,
                "metrics_reporter_url": f"{self.http.url}/metrics",
            })

    """
    Validates key availability properties of the system using a single
    partition.
    """

    @cluster(num_nodes=4)
    def test_redpanda_metrics_reporting(self):
        """
        Testing if when fetching from single node all partitions are 
        returned in round robin fashion
        """
        # setup http server
        self.http.start()
        self.redpanda.start()

        total_topics = 5
        total_partitions = 0
        for _ in range(0, total_topics):
            partitions = random.randint(1, 8)
            total_partitions += partitions
            self.redpanda.create_topic(
                [TopicSpec(partition_count=partitions, replication_factor=3)])

        # create topics
        self.redpanda.logger.info(
            f"created {total_topics} topics with {total_partitions} partitions"
        )

        def _state_up_to_date():
            if self.http.requests:
                r = json.loads(self.http.requests[-1]['body'])
                return r['topic_count'] == total_topics
            return False

        wait_until(_state_up_to_date, 20, backoff_sec=1)
        self.http.stop()
        metadata = [json.loads(r['body']) for r in self.http.requests]
        for m in metadata:
            self.redpanda.logger.info(m)

        def assert_fields_are_the_same(metadata, field):
            for m in metadata:
                lhs = m[field]
                rhs = metadata[0][field]
                if lhs != rhs:
                    self.logger.error(f"metadata {field} {lhs}!={rhs}")
            assert all(m[field] == metadata[0][field] for m in metadata)

        # cluster uuid and create timestamp should stay the same across requests
        assert_fields_are_the_same(metadata, 'cluster_uuid')
        assert_fields_are_the_same(metadata, 'cluster_created_ts')
        # get the last report
        last = metadata.pop()
        assert last['topic_count'] == total_topics
        assert last['partition_count'] == total_partitions
        nodes_meta = last['nodes']

        assert len(last['nodes']) == 3

        assert all('node_id' in n for n in nodes_meta)
        assert all('cpu_count' in n for n in nodes_meta)
        assert all('version' in n for n in nodes_meta)
        assert all('uptime_ms' in n for n in nodes_meta)
        assert all('is_alive' in n for n in nodes_meta)
        assert all('disks' in n for n in nodes_meta)
