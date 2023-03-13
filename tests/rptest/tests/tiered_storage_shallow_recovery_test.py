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
from rptest.services.redpanda import SISettings, MetricsEndpoint
from rptest.clients.types import TopicSpec
from rptest.clients.rpk import RpkTool
from rptest.utils.si_utils import BucketView
from rptest.services.admin import Admin
from rptest.utils.node_operations import NodeDecommissionWaiter
from rptest.utils.mode_checks import skip_debug_mode
import time


class TieredStorageShallowRecoveryTest(RedpandaTest):
    log_segment_size = 128 * 1024 * 1024
    segment_upload_interval = 30
    manifest_upload_interval = 10

    topics = (TopicSpec(replication_factor=3, partition_count=1), )

    def __init__(self, test_context, *args, **kwargs):
        self.si_settings = SISettings(
            test_context=test_context,

            # TODO; define a config that is small enough for docker nodes but
            # big enough that the read path doesn't constantly stall on
            # cache full.
            cloud_storage_cache_size=20 * 1024 * 1024 * 1024,
            log_segment_size=self.log_segment_size,
        )
        kwargs['si_settings'] = self.si_settings

        # Use interval uploads so that at end of test we may do an "everything
        # was uploaded" success condition.
        kwargs['extra_rp_conf'] = {
            # We do not intend to do interval-triggered uploads during produce,
            # but we set this property so that at the end of the test we may
            # do a simple "everything was uploaded" check after the interval.
            'cloud_storage_segment_max_upload_interval_sec':
            self.segment_upload_interval,
            # The test will assert that the number of manifest uploads does
            # not exceed what we would expect based on this interval.
            'cloud_storage_manifest_max_upload_interval_sec':
            self.manifest_upload_interval,

            # This test manually controls leadership location, we don't want
            # the balancer fighting with that.
            'enable_leader_balancer': False
        }
        super().__init__(test_context, *args, num_brokers=4, **kwargs)

    def setUp(self):
        pass

    def _sum_metrics(self, metric_name, nodes=None):
        samples = self.redpanda.metrics_sample(metric_name,
                                               nodes=nodes).samples
        self.logger.info(f"{metric_name} samples: (nodes {nodes})")
        for s in samples:
            self.logger.info(f"{s.value} {s.labels}")
        return sum(s.value for s in samples)

    @skip_debug_mode
    @cluster(num_nodes=5)
    def shallow_recovery_test(self):
        """
        Stress the tiered storage upload path on a single partition.  This
        corresponds to a workload in which the user does not create many
        partitions, and consequently will upload segments very frequently
        from a single partition.

        We are looking to ensure that the uploads keep up, and that the
        manifest uploads are done efficiently (e.g. not re-uploading
        the manifest with each segment).
        """

        local_read_bytes_metric = "vectorized_storage_log_read_bytes_total"
        remote_read_bytes_metric = "vectorized_cloud_storage_bytes_received_total"

        original_nodes = self.redpanda.nodes[0:3]
        self.redpanda.start(nodes=original_nodes)
        self._create_initial_topics()

        rpk = RpkTool(self.redpanda)

        produce_byte_rate = 50 * 1024 * 1024

        # Large messages, this is a bandwith test for uploads, we do not
        # want CPU handling of small records to be a bottleneck
        msg_size = 32768

        # Enough data to run for >5min to see some kind of steady state
        target_runtime = self.manifest_upload_interval * 3
        write_bytes = produce_byte_rate * target_runtime

        msg_count = write_bytes // msg_size

        # The producer should achieve throughput within this factor of what we asked for:
        # if this is violated then it is something wrong with the client or test environment.
        throughput_tolerance_factor = 2

        expect_duration = (write_bytes //
                           produce_byte_rate) * throughput_tolerance_factor

        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       self.topic,
                                       msg_size=msg_size,
                                       msg_count=msg_count,
                                       batch_max_bytes=512 * 1024,
                                       rate_limit_bps=produce_byte_rate)

        self.logger.info(f"Producing {msg_count} msgs ({write_bytes} bytes)")
        t1 = time.time()
        producer.start()
        producer.wait(timeout_sec=expect_duration)
        produce_duration = time.time() - t1
        actual_byte_rate = (write_bytes / produce_duration)
        mbps = int(actual_byte_rate / (1024 * 1024))
        self.logger.info(
            f"Produced {write_bytes} in {produce_duration}s, {mbps}MiB/s")

        # As a control to the later post-recovery read we will do, consume the whole
        # partition and check we service the read from local disk (minus however much
        # we service from batch cache)
        read_pre_consume = self._sum_metrics(local_read_bytes_metric,
                                             nodes=original_nodes)
        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          msg_count,
                                          nodes=producer.nodes,
                                          loop=False)
        consumer.start()
        consumer.wait()
        read_post_consume = self._sum_metrics(local_read_bytes_metric,
                                              nodes=original_nodes)
        self.logger.info(
            f"After first consume {read_pre_consume}->{read_post_consume} ({read_post_consume - read_pre_consume})"
        )

        # We expect reads to have been served from local disk.
        assert read_post_consume - read_pre_consume >= write_bytes

        # Producer should be within a factor of two of the intended byte rate, or something
        # is wrong with the test (running on nodes that can't keep up?) or with Redpanda
        # (some instability interrupted produce?)
        assert actual_byte_rate > produce_byte_rate / throughput_tolerance_factor
        # Check the workload is respecting rate limit
        assert actual_byte_rate < produce_byte_rate * throughput_tolerance_factor

        # Read the highest timestamp in local storage
        partition_describe = next(rpk.describe_topic(self.topic,
                                                     tolerant=True))
        hwm = partition_describe.high_watermark
        assert hwm >= msg_count
        consume_out = rpk.consume(topic=self.topic,
                                  n=1,
                                  offset=hwm - 1,
                                  partition=0,
                                  format="%d\\n")
        local_ts = int(consume_out.strip())
        self.logger.info(f"Max local ts = {local_ts}")

        # Measure how far behind the tiered storage uploads are: success condition
        # should be that they are within some time range of the most recently
        # produced data
        bucket = BucketView(self.redpanda)
        manifest = bucket.manifest_for_ntp(self.topic, 0)
        uploaded_ts = list(manifest['segments'].values())[-1]['max_timestamp']
        self.logger.info(f"Max uploaded ts = {uploaded_ts}")

        lag_seconds = (local_ts - uploaded_ts) / 1000.0
        self.logger.info(f"Upload lag: {lag_seconds}s")
        assert lag_seconds < (self.manifest_upload_interval +
                              (self.log_segment_size / actual_byte_rate))

        # Wait for all uploads to complete: this should take roughly segment_max_upload_interval_sec
        # plus manifest_max_upload_interval_sec
        def all_uploads_done():
            bucket.reset()
            manifest = bucket.manifest_for_ntp(self.topic, 0)
            top_segment = list(manifest['segments'].values())[-1]
            uploaded_ts = top_segment['max_timestamp']
            self.logger.info(f"Remote ts {uploaded_ts}, local ts {local_ts}")
            uploaded_raft_offset = top_segment['committed_offset']
            uploaded_kafka_offset = uploaded_raft_offset - top_segment[
                'delta_offset_end']
            self.logger.info(
                f"Remote HWM {uploaded_kafka_offset} (raft {uploaded_raft_offset}), local hwm {hwm}"
            )

            # -1 because uploaded offset is inclusive, hwm is exclusive
            return uploaded_kafka_offset >= (hwm - 1)

        self.redpanda.wait_until(all_uploads_done,
                                 timeout_sec=self.manifest_upload_interval +
                                 self.segment_upload_interval,
                                 backoff_sec=5)

        # We will simulate a clean node replacement: add a new node, then decom
        # the old node and wait for its workload to move to the node we added.
        remove_node = self.redpanda.nodes[0]
        add_node = self.redpanda.nodes[-1]

        # Now add a node to the cluster: some partitions should be rebalanced to
        # the added node, and those partitions should avoid doing full recovery
        # of data.
        self.redpanda.start_node(add_node)

        remove_node_id = self.redpanda.node_id(remove_node)
        add_node_id = self.redpanda.node_id(add_node)

        admin = Admin(self.redpanda)
        admin.decommission_broker(id=remove_node_id)

        waiter = NodeDecommissionWaiter(self.redpanda,
                                        remove_node_id,
                                        self.logger,
                                        progress_timeout=60)
        waiter.wait_for_removal()

        # Move leadership to the newly added node: it should be able to serve
        # reads for the whole partition history, albeit almost all via tiered storage
        admin.partition_transfer_leadership("kafka",
                                            self.topic,
                                            0,
                                            target_id=add_node_id)

        local_read_pre_consume = self._sum_metrics(local_read_bytes_metric,
                                                   nodes=[add_node])
        remote_read_pre_consume = self._sum_metrics(remote_read_bytes_metric,
                                                    nodes=[add_node])

        consumer = KgoVerifierSeqConsumer(self.test_context,
                                          self.redpanda,
                                          self.topic,
                                          0,
                                          msg_count,
                                          nodes=producer.nodes,
                                          loop=False)
        consumer.start()
        consumer.wait()

        local_read_post_consume = self._sum_metrics(local_read_bytes_metric,
                                                    nodes=[add_node])
        remote_read_post_consume = self._sum_metrics(remote_read_bytes_metric,
                                                     nodes=[add_node])

        self.logger.info(
            f"After consume local {local_read_pre_consume}->{local_read_post_consume} ({local_read_post_consume - local_read_pre_consume})"
        )
        self.logger.info(
            f"After consume remote {remote_read_pre_consume}->{remote_read_post_consume} ({remote_read_post_consume - remote_read_pre_consume})"
        )

        # We should have read from local, not remote storage
        assert local_read_post_consume - local_read_pre_consume < write_bytes
        assert remote_read_post_consume - remote_read_pre_consume >= write_bytes

        # TODO: switch leadership back to one of the other nodes, it should still
        # serve reads from local disk.

        # TODO: proceed to do a rolling remove/add of all the nodes as we would
        # see in a kubernetes environment: the end state will be for all nodes to
        # have very little local history, but everything should work.
