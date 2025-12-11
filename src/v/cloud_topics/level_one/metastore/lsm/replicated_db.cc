/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_persistence.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/logger.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/proto/manifest.proto.h"
#include "model/batch_builder.h"
#include "model/record.h"
#include "serde/rw/scalar.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
replicated_database::errc map_stm_error(stm::errc e) {
    switch (e) {
    case stm::errc::not_leader:
        return replicated_database::errc::not_leader;
    case stm::errc::raft_error:
        return replicated_database::errc::replication_error;
    case stm::errc::shutting_down:
        return replicated_database::errc::shutting_down;
    }
}
} // namespace

ss::future<std::expected<
  std::unique_ptr<replicated_database>,
  replicated_database::errc>>
replicated_database::open(
  stm* s,
  const std::filesystem::path& staging_directory,
  cloud_io::remote* remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as) {
    auto term_result = co_await s->sync(std::chrono::seconds(30));
    if (!term_result.has_value()) {
        co_return std::unexpected(map_stm_error(term_result.error()));
    }
    auto term = term_result.value();
    uint64_t epoch = term() + s->state().db_epoch_delta;

    vlog(
      cd_log.info,
      "Opening replicated LSM database for term {} with DB epoch {}",
      term,
      epoch);
    if (s->state().domain_uuid().is_nil()) {
        auto new_domain_uuid = domain_uuid(uuid_t::create());
        vlog(
          cd_log.info,
          "Replicating new domain UUID {} in term {}",
          new_domain_uuid,
          term);
        auto update = set_domain_uuid_update::build(
          s->state(), new_domain_uuid);
        model::batch_builder builder;
        builder.set_batch_type(model::record_batch_type::l1_stm);
        builder.add_record(
          {.key = serde::to_iobuf(lsm_update_key::set_domain_uuid),
           .value = serde::to_iobuf(update.value())});
        auto batch = co_await std::move(builder).build();
        auto replicate_result = co_await s->replicate_and_wait(
          term, std::move(batch), as);

        if (!replicate_result.has_value()) {
            vlog(
              cd_log.warn,
              "Failed to replicate set_domain_uuid batch: {}",
              int(replicate_result.error()));
            co_return std::unexpected(map_stm_error(replicate_result.error()));
        }
        if (s->state().domain_uuid().is_nil()) {
            co_return std::unexpected(errc::replication_error);
        }
    }
    cloud_storage_clients::object_key domain_prefix{
      fmt::format("{}", s->state().domain_uuid)};

    auto data_persist = co_await lsm::io::open_cloud_data_persistence(
      staging_directory, remote, bucket, domain_prefix);
    auto meta_persist = co_await open_replicated_metadata_persistence(
      s, remote, bucket, domain_prefix);
    lsm::io::persistence io{
      .data = std::move(data_persist),
      .metadata = std::move(meta_persist),
    };

    // Open the LSM database using the persisted manifest from the STM.
    auto db = co_await lsm::database::open(
      lsm::options{
        .database_epoch = epoch,
      },
      std::move(io));

    // Apply any volatile_buffer writes to the database.
    //
    // These are writes that were replicated but not yet persisted to the
    // manifest.
    auto max_persisted_seqno = db.max_persisted_seqno();
    if (!s->state().volatile_buffer.empty()) {
        vlog(
          cd_log.info,
          "Applying {} volatile writes to LSM database",
          s->state().volatile_buffer.size());

        auto wb = db.create_write_batch();
        size_t num_written = 0;
        for (const auto& row : s->state().volatile_buffer) {
            auto seqno = row.seqno;
            if (seqno <= max_persisted_seqno) {
                continue;
            }
            wb.put(row.row.key, row.row.value.copy(), seqno);
            vlog(
              cd_log.trace,
              "Replaying at seqno: {}, key: {}",
              seqno,
              row.row.key);
            ++num_written;
        }
        if (num_written > 0) {
            auto write_fut = co_await ss::coroutine::as_future(
              db.apply(std::move(wb)));
            if (write_fut.failed()) {
                auto ex = write_fut.get_exception();
                vlog(cd_log.error, "Failed to apply volatile writes: {}", ex);
                co_return std::unexpected(errc::io_error);
            }
        }
    }
    co_return std::unique_ptr<replicated_database>(new replicated_database(
      term,
      s,
      std::move(db),
      std::move(data_persist),
      std::move(meta_persist),
      as));
}

ss::future<std::expected<void, replicated_database::errc>>
replicated_database::close() {
    auto fut = co_await ss::coroutine::as_future(db_.close());
    if (fut.failed()) {
        auto ex = fut.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(errc::shutting_down);
        }
        co_return std::unexpected(errc::io_error);
    }
    co_return std::expected<void, errc>{};
}

bool replicated_database::needs_reopen() const {
    return !stm_->raft()->is_leader()
           || term_ != stm_->raft()->confirmed_term();
}

ss::future<std::expected<void, replicated_database::errc>>
replicated_database::write(chunked_vector<write_batch_row> rows) {
    if (rows.empty()) {
        co_return std::expected<void, errc>{};
    }

    auto update = apply_write_batch_update::build(
      stm_->state(), std::move(rows));

    if (!update.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to build write batch update: {}",
          update.error());
        co_return std::unexpected(errc::replication_error);
    }

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::l1_stm);
    builder.add_record(
      {.key = serde::to_iobuf(lsm_update_key::apply_write_batch),
       .value = serde::to_iobuf(update.value().copy())});
    auto batch = co_await std::move(builder).build();

    auto replicate_result = co_await stm_->replicate_and_wait(
      term_, std::move(batch), as_);

    if (!replicate_result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to replicate write batch: {}",
          int(replicate_result.error()));
        co_return std::unexpected(map_stm_error(replicate_result.error()));
    }

    auto wb = db_.create_write_batch();
    const auto seqno_delta = stm_->state().seqno_delta;
    auto seqno = lsm::sequence_number(replicate_result.value()() + seqno_delta);
    for (const auto& row : update.value().rows) {
        vlog(cd_log.trace, "Applying at seqno: {}, key: {}", seqno, row.key);
        if (row.value.empty()) {
            wb.remove(row.key, seqno);
        } else {
            wb.put(row.key, row.value.copy(), seqno);
        }
    }

    auto write_fut = co_await ss::coroutine::as_future(
      db_.apply(std::move(wb)));
    if (write_fut.failed()) {
        auto ex = write_fut.get_exception();
        vlog(cd_log.error, "Failed to write to LSM database: {}", ex);
        co_return std::unexpected(errc::io_error);
    }

    co_return std::expected<void, errc>{};
}

ss::future<std::expected<void, replicated_database::errc>>
replicated_database::reset(
  domain_uuid uuid, std::optional<lsm::proto::manifest> manifest) {
    if (uuid == stm_->state().domain_uuid) {
        co_return std::expected<void, errc>{};
    }
    auto update = reset_manifest_update::build(
      stm_->state(), uuid, std::move(manifest));

    if (!update.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to build reset_manifest update: {}",
          update.error());
        co_return std::unexpected(errc::replication_error);
    }

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::l1_stm);
    builder.add_record(
      {.key = serde::to_iobuf(lsm_update_key::reset_manifest),
       .value = co_await serde::to_iobuf_async(std::move(update.value()))});
    auto batch = co_await std::move(builder).build();

    auto replicate_result = co_await stm_->replicate_and_wait(
      term_, std::move(batch), as_);

    if (!replicate_result.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to replicate manifest reset: {}",
          int(replicate_result.error()));
        co_return std::unexpected(map_stm_error(replicate_result.error()));
    }
    if (uuid != stm_->state().domain_uuid) {
        co_return std::unexpected(errc::replication_error);
    }

    co_return std::expected<void, errc>{};
}

domain_uuid replicated_database::get_domain_uuid() const {
    return stm_->state().domain_uuid;
}

ss::future<std::expected<void, replicated_database::errc>>
replicated_database::flush() {
    // TODO: plumb a timeout here? Test for metadata update failures?
    auto flush_fut = co_await ss::coroutine::as_future(db_.flush());
    if (flush_fut.failed()) {
        auto ex = flush_fut.get_exception();
        vlog(cd_log.error, "Failed to flush to LSM database: {}", ex);
        co_return std::unexpected(errc::io_error);
    }
    co_return std::expected<void, errc>{};
}

} // namespace cloud_topics::l1
