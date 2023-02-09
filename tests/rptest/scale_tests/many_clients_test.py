# Copyright 2022 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import ResourceSettings, RESTART_LOG_ALLOW_LIST, LoggingConfig
from rptest.services.cluster import cluster
from rptest.services.rpk_consumer import RpkConsumer

from ducktape.utils.util import wait_until

from rptest.services.producer_swarm import ProducerSwarm

resource_settings = ResourceSettings(
    num_cpus=2,

    # Set a low memory size, such that there is only ~100k of memory available
    # for dealing with each client.
    memory_mb=768)


class ManyClientsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        # We will send huge numbers of messages, so tune down the log verbosity
        # as this is just a "did we stay up?" test
        if True:
            kwargs['log_config'] = LoggingConfig(default_level="info",
                                                 logger_levels={
                                                     'kafka': 'trace',
                                                     'storage': 'debug'
                                                 })
        else:
            kwargs['log_level'] = 'info'
        kwargs['resource_settings'] = resource_settings
        kwargs['extra_rp_conf'] = {
            # Enable segment size jitter as this is a stress test and does not
            # rely on exact segment counts.
            'log_segment_size_jitter_percent': 5,
            # This limit caps the produce throughput to a sustainable rate for a RP
            # cluster that has 384MB of memory per shard. It is set here to
            # since our current backpressure mechanisms will allow producers to
            # produce at a much higher rate and cause RP to run out of memory.
            #'target_quota_byte_rate':
            #31460000,  # 30MiB/s of throughput per shard
            # Same intention as above but utilizing node-wide throughput limit
            #'kafka_throughput_limit_node_in_bps':
            #27857600,  # 100MiB/s per node
        }
        super().__init__(*args, **kwargs)

    @cluster(num_nodes=6)
    def test_many_clients(self):
        """
        Check that redpanda remains stable under higher numbers of clients
        than usual.
        """

        # Scale tests are not run on debug builds
        assert not self.debug_mode

        PARTITION_COUNT = 100
        PRODUCER_COUNT = 4000
        PRODUCER_TIMEOUT_MS = 5000
        TOPIC_NAME = "manyclients"
        RECORDS_PER_PRODUCER = 1000

        # Realistic conditions: 128MB is the segment size in the cloud
        segment_size = 128 * 1024 * 1024
        retention_size = 2 * segment_size

        self.client().create_topic(
            TopicSpec(name=TOPIC_NAME,
                      partition_count=PARTITION_COUNT,
                      retention_bytes=retention_size,
                      segment_bytes=segment_size))

        # Two consumers, just so that we are at least touching consumer
        # group functionality, if not stressing the overall number of consumers.
        consumer_a = RpkConsumer(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 group="testgroup",
                                 save_msgs=False)
        consumer_b = RpkConsumer(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 group="testgroup",
                                 save_msgs=False)

        producer = ProducerSwarm(self.test_context,
                                 self.redpanda,
                                 TOPIC_NAME,
                                 PRODUCER_COUNT,
                                 RECORDS_PER_PRODUCER,
                                 timeout_ms=PRODUCER_TIMEOUT_MS)
        producer.start()
        consumer_a.start()
        consumer_b.start()

        producer.wait()

        def complete():
            expect = PRODUCER_COUNT * RECORDS_PER_PRODUCER
            self.logger.info(
                f"Message counts: {consumer_a.message_count} {consumer_b.message_count} (vs {expect})"
            )
            return consumer_a.message_count + consumer_b.message_count >= expect

        self.redpanda.wait_until(complete,
                                 timeout_sec=30,
                                 backoff_sec=1,
                                 err_msg="Consumers didn't see all messages")
