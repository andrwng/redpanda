/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "lsm/lsm.h"
#include "lsm/proto/manifest.proto.h"
#include "serde/envelope.h"
#include "serde/read_header.h"

#include <deque>

namespace cloud_topics::l1 {

struct volatile_row
  : public serde::
      envelope<volatile_row, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const volatile_row&, const volatile_row&) = default;
    auto serde_fields() { return std::tie(seqno, row); }
    lsm::sequence_number seqno;
    write_batch_row row;
};

// State that backs a replicated database on object storage. This comprises of:
// - A manifest that has been uploaded to object storage that all replicas will
//   be able to open a database from.
// - A list of writes that need to be replayed on top of the manifest in order
//   to be caught up. This list can be thought of as a write-ahead log for the
//   database.
struct lsm_state
  : public serde::
      envelope<lsm_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const lsm_state&, const lsm_state&) = default;
    ss::future<> serde_async_write(iobuf&) const;
    ss::future<> serde_async_read(iobuf_parser&, const serde::header&);
    ss::future<lsm_state> copy() const;

    // The unique identifier for this LSM state. This should be used as the
    // basis for where the database writes its data and metadata.
    //
    // Must be set before any writes are accepted. May be reset if recovering
    // from an existing manifest that is written with a different domain path
    // (e.g.  when recovering state from cloud).
    domain_uuid domain_uuid{};

    // Difference between Raft offset/term and database seqno/epoch. These will
    // be non-zero upon recovery from object storage, since the restored
    // seqno/epoch will not start at zero in that case. These may also be
    // negative, in case recovery was performed on a non-empty Raft log.
    //
    // seqno = offset + seqno_delta
    // epoch = term + epoch_delta
    //
    // TODO: use named types to enforce correct arithemetic rules.
    int64_t seqno_delta{0};
    int64_t db_epoch_delta{0};

    // Rows that aren't persisted to object storage yet but should be applied
    // to the database. When opening a database from the persisted manifest,
    // these rows must be applied to the opened database to catch it up.
    std::deque<volatile_row> volatile_buffer;

    // State that is persisted to object storage. This state contains write
    // operations up to a given sequence number; operations from below that
    // sequence number can be removed from the volatile buffer and no longer
    // need to be replayed to the database when opening the database from this
    // state.
    std::optional<lsm::proto::manifest> persisted_manifest;
};

struct lsm_stm_snapshot
  : public serde::
      envelope<lsm_stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    lsm_state state;

    ss::future<> serde_async_write(iobuf&) const;
    ss::future<> serde_async_read(iobuf_parser&, const serde::header&);
};

} // namespace cloud_topics::l1
