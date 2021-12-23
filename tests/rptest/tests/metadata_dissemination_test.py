# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random
import threading
import concurrent.futures
import time
import uuid

from ducktape.mark.resource import cluster
from ducktape.mark import parametrize
from ducktape.utils.util import wait_until

from rptest.archival.s3_client import S3Client
from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.rpk_producer import RpkProducer


class MetadataDisseminationTest(RedpandaTest):
    PARTITION_COUNT = 1024
    topics = (TopicSpec(partition_count=PARTITION_COUNT,
                        replication_factor=3), )

    segment_size = 16 * 1048576
    s3_host_name = "minio-s3"
    s3_access_key = "panda-user"
    s3_secret_key = "panda-secret"
    s3_region = "panda-region"
    s3_topic_name = "panda-topic"

    def __init__(self, test_ctx, *args, **kwargs):
        self.s3_bucket_name = f"panda-bucket-{uuid.uuid1()}"
        self._ctx = test_ctx
        super(MetadataDisseminationTest, self).__init__(
            test_ctx,
            num_brokers=6,
            *args,
            extra_rp_conf={
                # Disable leader balancer to avoid interference in
                # our leader movements.
                'enable_leader_balancer': False,
                # Disable prometheus metrics, because we are doing lots
                # of restarts with lots of partitions, and current high
                # metric counts make that sometimes cause reactor stalls
                # during shutdown on debug builds.
                'disable_metrics': True,

                # We will run relatively large number of partitions
                # and want it to work with slow debug builds and
                # on noisy developer workstations: relax the raft
                # intervals
                'election_timeout_ms': 5000,
                'raft_heartbeat_interval_ms': 500,

                # Cloud storage config
                'log_segment_size': self.segment_size,
                'cloud_storage_enabled': True,
                'cloud_storage_access_key': self.s3_access_key,
                'cloud_storage_secret_key': self.s3_secret_key,
                'cloud_storage_region': self.s3_region,
                'cloud_storage_bucket': self.s3_bucket_name,
                'cloud_storage_disable_tls': True,
                'cloud_storage_api_endpoint': self.s3_host_name,
                'cloud_storage_api_endpoint_port': 9000,
                #'cloud_storage_reconciliation_interval_ms':500,
                #'cloud_storage_max_connections':5,
            },
            **kwargs)

        self.s3client = S3Client(
            region=self.s3_region,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            endpoint=f"http://{self.s3_host_name}:9000",
            logger=self.logger,
        )
        self.s3client.create_bucket(self.s3_bucket_name)

    def tearDown(self):
        failed_deletions = self.s3client.empty_bucket(self.s3_bucket_name)
        assert len(failed_deletions) == 0
        self.s3client.delete_bucket(self.s3_bucket_name)

    @cluster(num_nodes=7)
    @parametrize(enable_si=True, force_stop=True)
    @parametrize(enable_si=True, force_stop=False)
    @parametrize(enable_si=False, force_stop=False)
    def test_metadata_updates_on_restart(self, enable_si, force_stop):
        """
        On simultaneous restart of all nodes, the new leadership after restart
        should be properly reflected on all nodes.

        Important to run on >3 nodes so that we include nodes who are not members
        of the groups they report metadata on.
        """

        if enable_si:
            rpk = RpkTool(self.redpanda)
            rpk.alter_topic_config(self.topic, 'redpanda.remote.write', 'true')
            rpk.alter_topic_config(self.topic, 'redpanda.remote.read', 'true')
            rpk.alter_topic_config(self.topic, 'retention.bytes',
                                   str(self.segment_size * 2))

        n_iterations = 2
        for n in range(1, n_iterations + 1):
            self.logger.info(f"Iteration {n}/{n_iterations}")
            self._shuffle_and_check(force_stop)

        if enable_si:
            # Verify that we really enabled shadow indexing correctly, such
            # that some objects were written
            objects = list(self.s3client.list_objects(self.s3_bucket_name))
            assert len(objects) > 0
            for o in objects:
                self.logger.info(f"S3 object: {o.Key}, {o.ContentLength}")

    def _shuffle_and_check(self, force_stop):
        # Some paralellism when driving admin API, but don't go crazy: we don't
        # want to induce lots of timeouts etc
        api_concurrent = 16

        # Shuffle leadership of all partitions, so that on restart the leadership
        # will move (i.e. cluster will have to do a good job of synchronising, rather
        # than leadership just staying the same as it was before the restart)
        admin = Admin(self.redpanda)

        def all_have_leaders():
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=api_concurrent) as executor:
                check_futs = []
                for p in range(0, self.PARTITION_COUNT):
                    check_futs.append(
                        executor.submit(admin.get_partitions, self.topic, p))

                any_leaderless = False
                for f in check_futs:
                    partition = f.result()
                    if partition['leader_id'] == -1:
                        self.logger.info(
                            f"No leader yet for partition {partition['partition_id']}"
                        )
                        any_leaderless = True

                return not any_leaderless

        wait_until(all_have_leaders, timeout_sec=30, backoff_sec=5, err_msg="")

        # Run a producer concurrently with the redpanda restart
        producer = RpkProducer(self._ctx, self.redpanda, self.topic, 128000,
                               1000000)
        producer.start()

        def transfer_one(p):
            partition = admin.get_partitions(self.topic, p)
            replicas = [r['node_id'] for r in partition['replicas']]
            leader = partition['leader_id']
            replicas = list(set(replicas) - {leader})
            new_leader = random.choice(replicas)
            admin.partition_transfer_leadership(namespace="kafka",
                                                topic=self.topic,
                                                partition=p,
                                                target_id=new_leader)

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=api_concurrent) as executor:
            transfer_futs = []
            for p in range(0, self.PARTITION_COUNT):
                transfer_futs.append(executor.submit(transfer_one, p))

            for f in transfer_futs:
                f.result()

        time.sleep(5)

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=api_concurrent) as executor:
            check_futs = []
            n_moved = 0
            for p in range(0, self.PARTITION_COUNT):
                check_futs.append(
                    executor.submit(admin.get_partitions, self.topic, p))

            for f in check_futs:
                partition = f.result()
                leader = partition['leader_id']
                if leader != -1 and leader != partition['replicas'][0][
                        'node_id']:
                    n_moved += 1

        self.logger.info(
            f"Transferred {n_moved} partitions from their default leadership")

        # We only require that most moves succeed, for the metadata check after
        # restart to be a valid test
        assert n_moved > self.PARTITION_COUNT / 2

        # Important that we restart at same time, not one after the other
        class Restarter(threading.Thread):
            def __init__(self, redpanda, node):
                self.redpanda = redpanda
                self.node = node
                self.ex = None
                super(Restarter, self).__init__()

            def run(self):
                try:
                    # Use extra-long timeouts because we're not here to test startup and
                    # shutdown promptness: we're here to test the metadata afterwards
                    self.redpanda.restart_nodes([self.node],
                                                stop_timeout=60,
                                                start_timeout=60,
                                                force_stop=force_stop)
                except Exception as e:
                    self.ex = e

        restarters = []
        for n in self.redpanda.nodes:
            restarters.append(Restarter(self.redpanda, n))

        for r in restarters:
            r.start()

        for r in restarters:
            r.join()
            if r.ex is not None:
                self.logger.error(f"Error restarting: {r.ex}")
            assert r.ex is None

        # The producer probably died already due to the server becoming fully
        # unavailable, so stop it explicitly here to clear up.
        try:
            producer.stop()
        except Exception as e:
            self.logger.info(f"Ignoring RPK error on stopped cluster {e}")
        finally:
            producer.free()

        rpk = RpkTool(self.redpanda)

        # Issue kafka metadata request, all partitions should have valid
        # outputs.  describe_topic will omit lines it can't parse,
        # so if we get errors they'll be missing.
        def metadata_ok():
            partition_results = list(rpk.describe_topic(self.topic))
            ok = len(partition_results) == self.PARTITION_COUNT
            if not ok:
                self.logger.warn(f"Partition metadata incomplete:")
                for pr in partition_results:
                    self.logger.warn(f"{pr}")
            else:
                self.logger.info(f"Partition metadata OK:")
                any_gt_zero = False
                for pr in partition_results:
                    self.logger.info(f"{pr}")
                    if pr.high_watermark > 0:
                        any_gt_zero = True

                # Verify that we really produced something into the topic,
                # in case of silently ignored RPK errors
                assert any_gt_zero

            return ok

        wait_until(metadata_ok, timeout_sec=30, backoff_sec=5, err_msg="")
