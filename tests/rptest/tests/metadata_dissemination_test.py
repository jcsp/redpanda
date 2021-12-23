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

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool, RpkException
from rptest.clients.types import TopicSpec
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.rpk_producer import RpkProducer


class MetadataDisseminationTest(RedpandaTest):
    PARTITION_COUNT = 1000
    topics = (TopicSpec(partition_count=PARTITION_COUNT,
                        replication_factor=3), )

    def __init__(self, test_ctx, *args, **kwargs):
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
            },
            **kwargs)

    @cluster(num_nodes=7)
    def test_metadata_updates_on_restart(self):
        """
        On simultaneous restart of all nodes, the new leadership after restart
        should be properly reflected on all nodes.

        Important to run on >3 nodes so that we include nodes who are not members
        of the groups they report metadata on.
        """

        producer = RpkProducer(self._ctx, self.redpanda, self.topic, 128000,
                               1000000)
        producer.start()

        n_iterations = 10
        for n in range(1, n_iterations + 1):
            self.logger.info(f"Iteration {n}/{n_iterations}")
            self._shuffle_and_check()

        producer.stop()
        producer.wait()

    def _shuffle_and_check(self):
        # Shuffle leadership of all partitions, so that on restart the leadership
        # will move (i.e. cluster will have to do a good job of synchronising, rather
        # than leadership just staying the same as it was before the restart)
        admin = Admin(self.redpanda)

        def transfer_one(p):
            partition = admin.get_partitions(self.topic, p)
            replicas = [r['node_id'] for r in partition['replicas']]
            leader = partition['leader_id']
            replicas = list(set(replicas) - {leader})
            new_leader = random.choice(replicas)
            admin.transfer_leadership_to(namespace="kafka",
                                         topic=self.topic,
                                         partition=p,
                                         target=new_leader)

        with concurrent.futures.ThreadPoolExecutor() as executor:
            transfer_futs = []
            for p in range(0, self.PARTITION_COUNT):
                transfer_futs.append(executor.submit(transfer_one, p))

            for f in transfer_futs:
                f.result()

        with concurrent.futures.ThreadPoolExecutor() as executor:
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
                                                start_timeout=60)
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

        rpk = RpkTool(self.redpanda)

        # Issue kafka metadata request, all partitions should have valid
        # outputs.  describe_topic will omit lines it can't parse,
        # so if we get errors they'll be missing.
        def metadata_ok():
            partition_results = rpk.describe_topic(self.topic)
            ok = len(list(partition_results)) == self.PARTITION_COUNT
            if not ok:
                self.logger.warn(f"Partition metadata incomplete:")
                for pr in partition_results:
                    self.logger.warn(f"{pr}")
            else:
                self.logger.info(f"Partition metadata OK:")
                for pr in partition_results:
                    self.logger.info(f"{pr}")

            return ok

        wait_until(metadata_ok, timeout_sec=30, backoff_sec=5, err_msg="")
