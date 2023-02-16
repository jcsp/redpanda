from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.redpanda import RESTART_LOG_ALLOW_LIST, SISettings
from rptest.scale_tests.many_partitions_test import ScaleParameters
from rptest.services.kgo_repeater_service import repeater_traffic
from rptest.clients.rpk import RpkTool


class RestartStressTest(RedpandaTest):
    def _repeater_worker_count(self, scale):
        workers = 32 * scale.node_cpus
        if self.redpanda.dedicated_nodes:
            # 768 workers on a 24 core node has been seen to work well.
            return workers
        else:
            return min(workers, 4)

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(
            test_context,
            *args,
            #si_settings=SISettings(test_context),
            **kwargs)

    def setUp(self):
        pass

    @cluster(num_nodes=6, log_allow_list=RESTART_LOG_ALLOW_LIST)
    def test_restart_stress(self):
        """
        Look for shutdown issues but restarting redpanda repeatedly under load,

        """
        scale = ScaleParameters(self.redpanda,
                                replication_factor=3,
                                tiered_storage_enabled=True)

        # Override the ScaleParamters default tiered storage configuration: it
        # is built for generating pathologically tiny segments in ManyPartitionsTest
        scale.segment_size = 1 * 1024 * 1024
        scale.retention_bytes = scale.segment_size * 10
        scale.local_retention_bytes = scale.segment_size * 2

        # We want "large" partition count but this test is not about testing
        # that limit, so dial it down to 50% the max.
        partition_count = scale.partition_limit // 2

        # Start cluster
        self.redpanda.start()

        repeater_topic = "t-repeater"
        repeater_topic_config = {
            'cleanup.policy': 'delete',
            'segment.bytes': str(scale.segment_size),
            'retention.bytes': str(scale.retention_bytes),
            'retention.local.bytes': scale.local_retention_bytes
        }
        rpk = RpkTool(self.redpanda)
        rpk.create_topic(repeater_topic,
                         partitions=partition_count,
                         replicas=3,
                         config=repeater_topic_config)

        restarts = 10

        # XXX hack for John's workstation
        scale.expect_bandwidth = 200 * 1024 * 1024

        # TODO: parametrize test and/or always run "fully loaded" once everything
        # is stable.
        maintenance_mode = True
        transactions = False

        repeater_args = {}
        if transactions:
            repeater_args = repeater_args | {
                'use_transactions': True,
                'transaction_abort_rate': 0.01
            }

        with repeater_traffic(
                context=self.test_context,
                redpanda=self.redpanda,
                topic=repeater_topic,
                msg_size=32768,
                workers=self._repeater_worker_count(scale),
                max_buffered_records=16,
                num_nodes=3,
                #rate_limit_bps=scale.expect_bandwidth,
                **repeater_args,
        ) as repeater:
            repeater.await_group_ready()
            for i in range(1, restarts + 1):
                self.logger.info(f"Restart round {i}/{restarts}")
                for n in self.redpanda.nodes:
                    # Always wait for some progress between node restarts.  This
                    # timeout is long because progress might have to wait for consumer
                    # groups to reform
                    repeater.await_progress(100, 60)
                    self.logger.info(f"Restarting node {n.name}")

                    # This will time out and fail the test if restarting takes too long
                    if maintenance_mode:
                        self.redpanda.rolling_restart_nodes(nodes=[n])
                    else:
                        self.redpanda.restart_nodes(nodes=[n])

        rpk.delete_topic(repeater_topic)
