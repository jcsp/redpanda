# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests

from rptest.tests.redpanda_test import RedpandaTest
from ducktape.mark.resource import cluster

BOOTSTRAP_CONFIG = {
    # A non-default value for checking bootstrap import works
    'enable_idempotence': True,
}


class ApiDocsTest(RedpandaTest):
    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(ApiDocsTest, self).__init__(test_ctx,
                                          num_brokers=1,
                                          *args,
                                          extra_rp_conf={},
                                          **kwargs)

    @cluster(num_nodes=1)
    def admin_docs_test(self):
        url = f"http://{self.redpanda.nodes[0].account.hostname}:9644/v1"

        r = requests.get(url)
        r.raise_for_status()
        _ = r.json()

    @cluster(num_nodes=1)
    def pandaproxy_docs_test(self):
        url = f"http://{self.redpanda.nodes[0].account.hostname}:8082"

        r = requests.get(url)
        r.raise_for_status()
        _ = r.json()

    @cluster(num_nodes=1)
    def schema_registry_docs_test(self):
        url = f"http://{self.redpanda.nodes[0].account.hostname}:8081"

        r = requests.get(url)
        r.raise_for_status()
        _ = r.json()
