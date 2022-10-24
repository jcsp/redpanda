// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/record_batch_reader.h"
#include "storage/api.h"
#include "storage/directories.h"
#include "storage/disk_log_appender.h"
#include "storage/segment_appender.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_reader.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

using namespace std::chrono_literals; // NOLINT
using namespace storage;              // NOLINT

log_config make_config() {
    return log_config{
      log_config::storage_type::disk,
      "scratch",
      1024,
      debug_sanitize_files::yes};
}

ntp_config config_from_ntp(const model::ntp& ntp) {
    return ntp_config(ntp, "scratch", {}, model::revision_id{61});
}

SEASTAR_THREAD_TEST_CASE(test_scratch) {
  auto conf = make_config();
  storage::api store(
  [conf]() {
      return storage::kvstore_config(
        1_MiB,
        config::mock_binding(10ms),
        conf.base_dir,
        storage::debug_sanitize_files::yes);
  },
  [conf]() { return conf; });
  store.start().get();
  auto stop_kvstore = ss::defer([&store] { store.stop().get(); });
  auto& m = store.log_mgr();

  auto log = m.manage(config_from_ntp(model::ntp{model::kafka_namespace, model::topic{"GainCapital_Execution_V1_Trade_EnrichedTrade"}, model::partition_id{15}})).get();

  /**
   *

   timestamps from the live system:
1666564213889
1666564217460
1666564218792
1666580600323
1666580602065
1666580604281
1666580611626
1666580615856
1666580618941
1666580619332
1666580655803
1666580655971
1666594879190
1666595026733
1666595092669
1666596496664
1666596496704
1666596496705
1666596496716
1666596496777
1666596496778
1666596496779
1666596496829
1666596496830
1666596496830
1666596496877
1666596496921
1666604414143
1666604414143
1666604414143
1666604419351
1666604420220
1666604420884
1666604421444
1666604481426
1666604493653
1666604871501
1666609753321


   Recovered last segment base_timestamp:{timestamp: 1666596023920}, max_timestamp:{timestamp: 1666611634057},

If I delete index files and let it rebuild:
INFO  2022-10-24 14:08:34,452 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74177-1386-v1.base_index, offsets:{74177}, index:{header_bitflags:0, base_offset:{74177}, max_offset:{74192}, base_timestamp:{timestamp: 1666560338284}, max_timestamp:{timestamp: 1666595092669}, index(1,1,1)}, step:32768, needs_persistence:0}
INFO  2022-10-24 14:08:34,452 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74193-1387-v1.base_index, offsets:{74193}, index:{header_bitflags:0, base_offset:{74193}, max_offset:{74193}, base_timestamp:{timestamp: 1666595991839}, max_timestamp:{timestamp: 1666595991839}, index(1,1,1)}, step:32768, needs_persistence:0}
INFO  2022-10-24 14:08:34,452 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74194-1388-v1.base_index, offsets:{74194}, index:{header_bitflags:0, base_offset:{74194}, max_offset:{74223}, base_timestamp:{timestamp: 1666596023920}, max_timestamp:{timestamp: 1666611634057}, index(1,1,1)}, step:32768, needs_persistence:0}

If I decode what's in there
INFO  2022-10-24 14:09:22,598 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74177-1386-v1.base_index, offsets:{74177}, index:{header_bitflags:0, base_offset:{74177}, max_offset:{74192}, base_timestamp:{timestamp: 1666560338284}, max_timestamp:{timestamp: 1666595092669}, index(1,1,1)}, step:32768, needs_persistence:0}
INFO  2022-10-24 14:09:22,598 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74193-1387-v1.base_index, offsets:{74193}, index:{header_bitflags:0, base_offset:{74193}, max_offset:{74193}, base_timestamp:{timestamp: 1666595991839}, max_timestamp:{timestamp: 1666595991839}, index(1,1,1)}, step:32768, needs_persistence:0}
INFO  2022-10-24 14:09:22,598 [shard 0] storage - disk_log_impl.cc:976 - tq segment: {file:scratch/kafka/GainCapital_Execution_V1_Trade_EnrichedTrade/15_61/74194-1388-v1.base_index, offsets:{74194}, index:{header_bitflags:0, base_offset:{74194}, max_offset:{74223}, base_timestamp:{timestamp: 1666596023920}, max_timestamp:{timestamp: 1666611634057}, index(1,1,1)}, step:32768, needs_persistence:0}

Consider one segment
base_offset:{74177}, max_offset:{74192}, base_timestamp:{timestamp: 1666560338284}, max_timestamp:{timestamp: 1666595092669}
The first batches within it:
batch_type::raft_configuration 74177  {timestamp: 1666560338284}-{timestamp: 1666560338284}
batch_type::raft_data 74178  {timestamp: 1666564213889}-{timestamp: 1666564213889}
batch_type::raft_data 74179  {timestamp: 1666564217460}-{timestamp: 1666564217460}
batch_type::raft_data 74180  {timestamp: 1666564218792}-{timestamp: 1666564218792}


*/

  storage::timequery_config tq_config{
        model::timestamp(1666564213889),
        model::offset{99999999999999},
        ss::default_priority_class(),
        {model::record_batch_type::raft_data},
        std::nullopt
  };

  storage::log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<int64_t>::max(),
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
  auto reader = log.make_reader(reader_cfg).get();
  std::cerr << "Reading all batches" << std::endl;
  auto batches = model::consume_reader_to_memory(std::move(reader), model::no_timeout).get();
  std::cerr << fmt::format("Read {} batches", batches.size()) << std::endl;
  for (const auto &b : batches) {
      std::cerr << fmt::format("b {} {} {}-{}", b.header().type, b.header().base_offset, b.header().first_timestamp, b.header().max_timestamp) << std::endl;
  }

  auto tq_result = log.timequery(tq_config).get();
  std::cerr << "has_value: " << tq_result.has_value() << std::endl;
  if (tq_result) {
      std::cerr << "tq_result: " << *tq_result << std::endl;
  }


  BOOST_REQUIRE(tq_result.has_value());
  BOOST_REQUIRE(tq_result->offset==model::offset{74178});



}
