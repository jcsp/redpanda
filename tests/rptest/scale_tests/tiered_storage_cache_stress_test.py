# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer, KgoVerifierSeqConsumer
from rptest.services.redpanda import SISettings, MetricsEndpoint, ResourceSettings
from rptest.services.admin import Admin
from rptest.clients.rpk import RpkTool
from ducktape.mark import matrix
import time
from enum import Enum


class TieredStorageCacheStressTest(RedpandaTest):
    segment_upload_interval = 30
    manifest_upload_interval = 10

    def __init__(self, test_context, *args, **kwargs):

        super().__init__(test_context, *args, **kwargs)

    def setUp(self):
        # defer redpanda startup to the test
        pass

    @cluster(num_nodes=4)
    # @matrix(limit_mode=[LimitMode.bytes],
    #         log_segment_size=[1024 * 1024, 128 * 1024 * 1024])
    #@matrix(limit_mode=[LimitMode.bytes], log_segment_size=[128 * 1024])
    @matrix(log_segment_size=[1024 * 1024])
    def streaming_cache_test(self, limit_mode, log_segment_size):
        """
        Validate that reading data much  larger than the cache proceeds safely
        and promptly: this exercises the mode where we are reading some data,
        trimming cache, reading, trimming etc.
        """

        if self.redpanda.dedicated_nodes:
            partition_count = 128

            # Lowball expectation of bandwidth that should work reliably on
            # small instance types
            expect_bandwidth = 100E6

            # Make the cache at least this bug (avoid using very small caches when
            # the segment size is small)
            at_least_bytes = 20 * 1024 * 1024 * 1024
        else:
            # In general, this test should always be run on dedicated nodes: mini-me mode
            # for developers on fast workstations hacking on the test.
            #partition_count = 16
            #expect_bandwidth = 20E6
            # XXX HACK

            partition_count = 128
            expect_bandwidth = 150
            at_least_bytes = 10 * 1024 * 1024 * 1024

        topic_name = 'streaming-read'

        msg_size = 16384

        # Cache trim interval is 5 seconds
        # Effective streaming bandwidth is 20% of cache size every trim period
        size_limit = int(
            max((expect_bandwidth * 5) / 0.2,
                partition_count * log_segment_size))

        size_limit = max(size_limit, at_least_bytes)

        # TODO: update when chunked download path lands
        # size_limit = max((expect_bandwidth * 5) / 0.2, partition_count * chunk_size)

        if partition_count >= len(self.redpanda.nodes):
            # If we have multiple partitions then we are spreading load across multiple
            # nodes, and each node should need a proportionally smaller cache
            size_limit // len(self.redpanda.nodes)

        # Enough objects to fill with segments and have one index for each
        max_objects = 2 * (size_limit // log_segment_size)
        # TODO: update when chunked download path lands,
        # max_objects = (size_limit / log_segment_size) + size_limit / chunk_size

        # Use interval uploads so that tests can do a "wait til everything uploaded"
        # check if they want to.
        extra_rp_conf = {
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,
        }

        si_settings = SISettings(
            test_context=self.test_context,
            log_segment_size=log_segment_size,
        )

        si_settings.cloud_storage_cache_size = size_limit

        # Write 10x more data than fits in the cache, so that during a consume
        # cycle we are having to drop+rehydrate it all.
        data_size = size_limit * 10

        # Write 10x more than one segment size, so that we will be reading from
        # remote data instead of local data (local retention set to one segment)
        data_size = max(data_size, log_segment_size * 10 * partition_count)

        msg_count = data_size // msg_size

        # We will run with artificially constrained memory, to minimize use of
        # the batch cache and ensure that tiered storage reads are not using
        # egregious amounts of memory.  The memory is within the official system
        # requirements (2GB per core).  Use a small core count so that if we're
        # running on a system with plenty of cores, we don't spread out the
        # partitions such that each core has lots of slack memory.
        self.redpanda.set_resource_settings(
            ResourceSettings(memory_mb=4096, num_cpus=2))

        self.redpanda.set_extra_rp_conf(extra_rp_conf)
        self.redpanda.set_si_settings(si_settings)
        self.redpanda.start()

        rpk = RpkTool(self.redpanda)
        rpk.create_topic(
            topic_name,
            partitions=partition_count,
            replicas=3,
            config={
                # Minimal local retention, to send traffic to remote
                # storage.
                'retention.local.target.bytes': log_segment_size,
            })

        self.logger.info(
            f"Writing {data_size} bytes, will be read using {size_limit}/{max_objects} cache"
        )

        # Sanity check test parameters against the nodes we are running on
        assert self.redpanda.get_node_disk_free() >= size_limit + data_size

        # Write out the data.  We will write + read in separate phases in order
        # that this test cleanly exercises the read path.
        t1 = time.time()
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            topic_name,
            msg_size=msg_size,
            msg_count=data_size // msg_size,
            batch_max_bytes=msg_size * 8,
            timeout_sec=(data_size / expect_bandwidth) * 2)
        produce_duration = time.time() - t1
        self.logger.info(
            f"Produced {data_size} bytes in {produce_duration} seconds, {(data_size/produce_duration)/1000000.0:.2f}MB/s"
        )

        # TODO wait for uploads to complete: they almost certainly are, but
        # let's make the test run like clockwork.

        # TODO: wrap the consume phase in a loop and do it several (10?) times,
        # to give a good chance of hitting any issues with accounting that build up
        # over many trim cyles

        # Read all the data, validate that we read complete and achieve
        # the streaming bandwidth that we expect
        t1 = time.time()
        expect_duration = data_size // expect_bandwidth
        self.logger.info(
            f"Consuming, expected duration {expect_duration:.2f}s")
        consumer = KgoVerifierSeqConsumer.oneshot(self.test_context,
                                                  self.redpanda,
                                                  topic_name,
                                                  loop=False,
                                                  timeout_sec=expect_duration *
                                                  2)
        assert consumer.consumer_status.validator.valid_reads == msg_count
        assert consumer.consumer_status.validator.invalid_reads == 0
        assert consumer.consumer_status.validator.out_of_scope_invalid_reads == 0
        consume_duration = time.time() - t1
        consume_rate = data_size / consume_duration
        self.logger.info(
            f"Consumed {data_size} bytes in {consume_duration} seconds, {consume_rate/1000000.0:.2f}MB/s"
        )

        # If we are not keeping up, it indicates an issue with trimming logic, such as
        # backing off too much or not trimming enough each time: there is a generous
        # 2x margin to make the test robust: if this _still_ fails, something is up.
        assert consume_rate > expect_bandwidth / 2

        # Validate that the cache end state is within configured limit
        admin = Admin(self.redpanda)
        any_cache_usage = False
        for node in self.redpanda.nodes:
            self.logger.info(
                f"Validating node {node.name} cache vs limits {size_limit}/{max_objects}"
            )

            # Read logical space usage according to Redpanda
            usage = admin.get_local_storage_usage(node)
            self.logger.info(f"Checking cache usage on {node.name}: {usage}")

            # Read physical space usage according to the operating system
            node_storage = self.redpanda.node_storage(node)
            self.logger.info(
                f"Checked physical cache usage on {node.name}: {node_storage.cache}"
            )

            # Read HWM stats from Redpanda, in case we transiently violated cache size
            # metrics = self.redpanda.metrics(
            #     node, metrics_endpoint=MetricsEndpoint.PUBLIC_METRICS)
            # hwm_size = None
            # for family in metrics:
            #     for sample in family.samples:
            #         if sample.name == "redpanda_cloud_storage_cache_space_hwm_size_bytes":
            #             hwm_size = int(sample.value)
            #         else:
            #             self.logger.debug(sample.name)
            # assert hwm_size is not None, "Cache HWM metric not found"

            any_cache_usage = any_cache_usage or (
                usage['cloud_storage_cache_bytes'] > 0
                and usage['cloud_storage_cache_objects'] > 0)

            assert usage['cloud_storage_cache_bytes'] <= size_limit
            assert node_storage.cache.bytes <= size_limit
            #assert hwm_size <= size_limit

        # At least one node should have _something_ in its cache, or something is wrong with our test
        assert any_cache_usage is True
