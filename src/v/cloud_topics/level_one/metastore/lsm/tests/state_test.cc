/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "bytes/bytes.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/tests/state_utils.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"

#include <seastar/core/coroutine.hh>
#include <seastar/util/log.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using ::testing::ElementsAre;
using ::testing::Pair;
using namespace cloud_topics::l1;

namespace {
ss::logger lg{"state_test"};
using o = kafka::offset;
using t = model::term_id;
using ts = model::timestamp;
const object_id oid1 = cloud_topics::l1::create_object_id();
const object_id oid2 = cloud_topics::l1::create_object_id();
const object_id oid3 = cloud_topics::l1::create_object_id();
const object_id oid4 = cloud_topics::l1::create_object_id();
const object_id oid5 = cloud_topics::l1::create_object_id();
const object_id oid6 = cloud_topics::l1::create_object_id();
const std::string_view tp1_str = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tp2_str = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tp3_str = "deadbeef-cccc-0000-0000-000000000000/0";
const auto tp1 = model::topic_id_partition::from(tp1_str);
const auto tp2 = model::topic_id_partition::from(tp2_str);
const auto tp3 = model::topic_id_partition::from(tp3_str);

ss::future<lsm::database> make_db() {
    using namespace ::lsm;
    auto db = co_await database::open(
      options{},
      io::persistence{
        .data = ::lsm::io::make_memory_data_persistence(),
        .metadata = ::lsm::io::make_memory_metadata_persistence(),
      });
    co_return std::move(db);
}

ss::future<> write_db(lsm::database& db, chunked_vector<write_batch_row> rows) {
    auto next_write_seqno = model::next_offset(db.max_applied_offset());
    auto wb = db.create_write_batch();
    for (auto& r : rows) {
        wb.put(std::move(r.key), std::move(r.value), next_write_seqno);
    }
    co_await db.apply(std::move(wb));
}

ss::future<chunked_vector<std::pair<std::string, std::string>>>
get_rows(lsm::database& db) {
    chunked_vector<std::pair<std::string, std::string>> ret;
    auto iter = co_await db.create_iterator();
    co_await iter.seek_to_first();
    while (iter.valid()) {
        ret.emplace_back(iter.key(), to_hex(iobuf_to_bytes(iter.value())));
        vlog(
          lg.info, "Key: {}, Value: {}", ret.back().first, ret.back().second);
        co_await iter.next();
    }
    co_return ret;
}

template<typename T>
std::string serde_hex(T t) {
    auto buf = serde::to_iobuf(std::move(t));
    return to_hex(iobuf_to_bytes(buf));
}

} // namespace

TEST(DatabaseStateUpdateTest, TestAddObjects) {
    auto db = make_db().get();
    {
        state_reader reader(db.create_snapshot());
        auto update = add_objects_db_update::from(
          add_objects_builder()
            .add(new_obj_builder(oid1, 100, 1100)
                   .add(tp1_str, o{0}, o{10}, ts{1999}, 0, 99)
                   .add(tp2_str, o{0}, o{10}, ts{1999}, 100, 199)
                   .build())
            .add_term_start(tp1_str, t{0}, o{0})
            .add_term_start(tp2_str, t{0}, o{0})
            .build());

        chunked_vector<write_batch_row> rows;
        auto build_res = update.build_rows(reader, rows).get();
        ASSERT_TRUE(build_res.has_value()) << build_res.error();
        ASSERT_NO_THROW(write_db(db, std::move(rows)).get());
    }
    auto rows = get_rows(db).get();
    EXPECT_THAT(
      rows,
      ElementsAre(
        Pair(
          metadata_row_key::encode(tp1),
          serde_hex(
            metadata_row_value{.start_offset = o{0}, .next_offset = o{11}})),
        Pair(
          metadata_row_key::encode(tp2),
          serde_hex(
            metadata_row_value{.start_offset = o{0}, .next_offset = o{11}})),
        Pair(
          extent_row_key::encode(tp1, o{0}),
          serde_hex(
            extent_row_value{
              .last_offset = o{10},
              .max_timestamp = ts{1999},
              .filepos = 0,
              .len = 99,
              .oid = oid1})),
        Pair(
          extent_row_key::encode(tp2, o{0}),
          serde_hex(
            extent_row_value{
              .last_offset = o{10},
              .max_timestamp = ts{1999},
              .filepos = 100,
              .len = 99,
              .oid = oid1})),
        Pair(
          term_row_key::encode(tp1, t{0}),
          serde_hex(term_row_value{.term_start_offset = o{0}})),
        Pair(
          term_row_key::encode(tp2, t{0}),
          serde_hex(term_row_value{.term_start_offset = o{0}})),
        Pair(
          object_row_key::encode(oid1),
          serde_hex(
            object_row_value{
              .object = object_entry{
                .total_data_size = 198,
                .removed_data_size = 0,
                .footer_pos = 100,
                .object_size = 1100}}))));
    db.close().get();
}
