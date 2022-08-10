# Copyright 2022 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.cluster import cluster
from rptest.services.openmessaging_benchmark import OpenMessagingBenchmark
from ducktape.mark import parametrize
from ducktape.mark import matrix


class OpenBenchmarkTest(RedpandaTest):
    BENCHMARK_WAIT_TIME_MIN = 10

    def __init__(self, ctx):
        self._ctx = ctx
        super(OpenBenchmarkTest, self).__init__(test_context=ctx,
                                                num_brokers=5)

    def setUp(self):
        # Defer redpanda startup
        pass

    @cluster(num_nodes=8)
    @matrix(driver=["REGRESSION_DRIVER"], workload=["LOAD_625k"])
    def test_balancing_on(self, driver, workload):
        self.redpanda.set_extra_rp_conf({
            "partition_autobalancing_node_availability_timeout_sec": 300,
            "partition_autobalancing_mode": "continuous",
        })
        self.redpanda.start()
        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda, driver,
                                           workload)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

    @cluster(num_nodes=8)
    @matrix(driver=["REGRESSION_DRIVER"], workload=["LOAD_625k"])
    def test_balancing_off(self, driver, workload):
        # Leave configs at default
        self.redpanda.start()

        benchmark = OpenMessagingBenchmark(self._ctx, self.redpanda, driver,
                                           workload)
        benchmark.start()
        benchmark_time_min = benchmark.benchmark_time(
        ) + OpenBenchmarkTest.BENCHMARK_WAIT_TIME_MIN
        benchmark.wait(timeout_sec=benchmark_time_min * 60)
        benchmark.check_succeed()

