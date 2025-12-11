/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state.h"

#include "serde/rw/envelope.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/sstring.h"
#include "serde/rw/uuid.h"
#include "serde/rw/vector.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
std::deque<volatile_row> copy_rows(const std::deque<volatile_row>& rows) {
    std::deque<volatile_row> copy;
    for (const auto& r : rows) {
        copy.push_back(
          volatile_row{
            .seqno = r.seqno,
            .row = write_batch_row{
              .key = r.row.key, .value = r.row.value.copy()}});
    }
    return copy;
}
} // namespace

ss::future<> lsm_state::serde_async_write(iobuf& out) const {
    using serde::write;
    write(out, domain_uuid);
    write(out, seqno_delta);
    write(out, db_epoch_delta);
    write(out, copy_rows(volatile_buffer));
    std::optional<iobuf> manifest_buf;
    if (persisted_manifest.has_value()) {
        manifest_buf = co_await persisted_manifest->to_proto();
    }
    write(out, std::move(manifest_buf));
}

ss::future<>
lsm_state::serde_async_read(iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    domain_uuid = read_nested<decltype(domain_uuid)>(in, h._bytes_left_limit);
    seqno_delta = read_nested<decltype(seqno_delta)>(in, h._bytes_left_limit);
    db_epoch_delta = read_nested<decltype(db_epoch_delta)>(
      in, h._bytes_left_limit);
    volatile_buffer = read_nested<decltype(volatile_buffer)>(
      in, h._bytes_left_limit);
    auto manifest_buf = read_nested<std::optional<iobuf>>(
      in, h._bytes_left_limit);
    if (manifest_buf.has_value()) {
        persisted_manifest = co_await lsm::proto::manifest::from_proto(
          std::move(*manifest_buf));
    }
}

ss::future<lsm_state> lsm_state::copy() const {
    // TODO: is there a better way to copy protos?
    std::optional<lsm::proto::manifest> manifest_copy;
    if (persisted_manifest.has_value()) {
        auto buf = co_await persisted_manifest->to_proto();
        manifest_copy = co_await lsm::proto::manifest::from_proto(
          std::move(buf));
    }
    co_return lsm_state{
      .domain_uuid = domain_uuid,
      .seqno_delta = seqno_delta,
      .db_epoch_delta = db_epoch_delta,
      .volatile_buffer = copy_rows(volatile_buffer),
      .persisted_manifest = std::move(manifest_copy),
    };
}

} // namespace cloud_topics::l1
