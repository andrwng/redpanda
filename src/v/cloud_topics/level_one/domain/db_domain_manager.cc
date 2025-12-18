/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/domain/db_domain_manager.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "container/chunked_hash_map.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/proto/manifest.proto.h"
#include "model/batch_builder.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/sleep.hh>

namespace cloud_topics::l1 {
namespace {
rpc::errc convert_stm_errc(stm::errc e) {
    switch (e) {
    case stm::errc::shutting_down:
    case stm::errc::not_leader:
        return rpc::errc::not_leader;
    case stm::errc::raft_error:
        return rpc::errc::timed_out;
    }
}

rpc::errc convert_db_errc(replicated_database::errc e) {
    switch (e) {
    case replicated_database::errc::io_error:
        return rpc::errc::timed_out;
    case replicated_database::errc::shutting_down:
    case replicated_database::errc::replication_error:
    case replicated_database::errc::not_leader:
        return rpc::errc::not_leader;
    }
}

} // namespace

db_domain_manager::db_domain_manager(ss::shared_ptr<stm> stm)
  : stm_(std::move(stm)) {}

void db_domain_manager::start() {
    ssx::spawn_with_gate(gate_, [this] { return gc_loop(); });
}

ss::future<> db_domain_manager::stop_and_wait() {
    vlog(cd_log.debug, "DB domain manager stopping...");
    as_.request_abort();
    co_await gate_.close();
    vlog(cd_log.debug, "DB domain manager stopped...");
}

std::optional<ss::gate::holder> db_domain_manager::maybe_gate() {
    ss::gate::holder h;
    if (as_.abort_requested() || gate_.is_closed()) {
        return std::nullopt;
    }
    return gate_.hold();
}

ss::future<rpc::add_objects_reply>
db_domain_manager::add_objects(rpc::add_objects_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    // Sync STM to ensure we're up-to-date
    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }

    // Build the database update
    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
    }

    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto update = add_objects_db_update{
      .new_objects = std::move(req.new_objects),
      .new_terms = std::move(req.new_terms),
    };
    auto writer_lock_res = co_await writer_lock();
    if (!writer_lock_res.has_value()) {
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    // Validate and build write batch rows
    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows, &corrections);
    if (!build_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to add objects: {}",
          build_res.error());
        co_return rpc::add_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }

    auto apply_res = co_await db_->write(std::move(rows));
    if (!apply_res.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to apply update to database: {}",
          apply_res.error());
        co_return rpc::add_objects_reply{
          .ec = convert_db_errc(apply_res.error()),
        };
    }

    co_return rpc::add_objects_reply{
      .ec = rpc::errc::ok,
      .corrected_next_offsets = std::move(corrections),
    };
}

ss::future<rpc::replace_objects_reply>
db_domain_manager::replace_objects(rpc::replace_objects_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }

    chunked_hash_set<object_id> added_oids;
    for (const auto& obj : req.new_objects) {
        added_oids.emplace(obj.oid);
    }
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      req_compaction_updates;
    for (auto& [tp, update] : req.compaction_updates) {
        const auto& t = tp.topic_id;
        const auto& p = tp.partition;
        req_compaction_updates[t][p] = std::move(update);
    }
    auto update = replace_objects_db_update::from(
      replace_objects_update{
        .new_objects = std::move(req.new_objects),
        .compaction_updates = std::move(req_compaction_updates),
      });

    auto init_db_res = co_await maybe_init_db();
    if (!init_db_res.has_value()) {
        // XXX: different code? Likely shutting down but unclear.
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto db_lock = co_await shared_db_lock();
    if (!db_lock.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    auto writer_lock_res = co_await writer_lock();
    if (!writer_lock_res.has_value()) {
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    auto reader = state_reader(db_->db().create_snapshot());
    chunked_vector<write_batch_row> rows;
    auto build_res = co_await update.build_rows(reader, rows);
    if (!build_res.has_value()) {
        vlog(
          cd_log.debug,
          "Rejecting request to replace objects: {}",
          build_res.error());
        co_return rpc::replace_objects_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    auto apply_res = co_await db_->write(std::move(rows));
    if (!apply_res.has_value()) {
        vlog(
          cd_log.warn,
          "Failed to apply update to database: {}",
          apply_res.error());
        co_return rpc::replace_objects_reply{
          .ec = convert_db_errc(apply_res.error()),
        };
    }

    // Verify objects were added
    if (!added_oids.empty()) {
        auto obj_res = co_await reader.get_object(*added_oids.begin());
        if (!obj_res.has_value() || !obj_res.value().has_value()) {
            co_return rpc::replace_objects_reply{
              .ec = rpc::errc::concurrent_requests,
            };
        }
    }

    co_return rpc::replace_objects_reply{
      .ec = rpc::errc::ok,
    };
}

ss::future<rpc::get_first_offset_ge_reply>
db_domain_manager::get_first_offset_ge(rpc::get_first_offset_ge_request req) {
    // TODO: Implement query using state_reader
    // This would involve:
    // 1. Get extent >= offset using state_reader.get_extent_ge()
    // 2. Get object metadata for the extent's oid
    // 3. Return object metadata
    auto gate_res = maybe_gate();
    if (!gate_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto init_db_res = co_await maybe_init_db();
    if (!init_db_res.has_value()) {
        // XXX: different code? Likely shutting down but unclear.
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto lock_res = co_await shared_db_lock();
    if (!lock_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto reader = state_reader(db_->db().create_snapshot());
    auto extent_res = co_await reader.get_extent_ge(req.tp, req.o);
    if (!extent_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    if (extent_res.value().has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    const auto& extent = extent_res.value().value();
    auto object_res = co_await reader.get_object(extent.oid);
    if (!object_res.has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    if (object_res.value().has_value()) {
        co_return rpc::get_first_offset_ge_reply{
          .ec = rpc::errc::out_of_range,
        };
    }
    const auto& object = object_res.value().value();
    co_return rpc::get_first_offset_ge_reply{
      .ec = rpc::errc::ok,
      .object = rpc::object_metadata{
        .oid = extent.oid,
        .footer_pos = object.footer_pos,
        .object_size = object.object_size,
        .first_offset = extent.base_offset,
        .last_offset = extent.last_offset,
      }};
}

ss::future<rpc::get_first_timestamp_ge_reply>
db_domain_manager::get_first_timestamp_ge(rpc::get_first_timestamp_ge_request) {
    // TODO: Implement using state_reader
    // Similar to get_first_offset_ge but also check timestamp

    co_return rpc::get_first_timestamp_ge_reply{
      .ec = rpc::errc::out_of_range, // Placeholder
    };
}

ss::future<rpc::get_first_offset_for_bytes_reply>
db_domain_manager::get_first_offset_for_bytes(
  rpc::get_first_offset_for_bytes_request) {
    // TODO: Implement using state_reader
    // Iterate extents, accumulate sizes until >= requested size

    co_return rpc::get_first_offset_for_bytes_reply{
      .ec = rpc::errc::out_of_range, // Placeholder
    };
}

ss::future<rpc::get_offsets_reply>
db_domain_manager::get_offsets(rpc::get_offsets_request) {
    // TODO: Implement using state_reader
    // Get metadata row for partition to get start_offset and next_offset

    co_return rpc::get_offsets_reply{
      .ec = rpc::errc::missing_ntp, // Placeholder
    };
}

ss::future<rpc::get_compaction_info_reply>
db_domain_manager::do_get_compaction_info(rpc::get_compaction_info_request) {
    // TODO: Implement using state_reader
    // Get compaction state, metadata, iterate extents to build response

    co_return rpc::get_compaction_info_reply{
      .ec = rpc::errc::missing_ntp, // Placeholder
    };
}

ss::future<rpc::get_compaction_info_reply>
db_domain_manager::get_compaction_info(rpc::get_compaction_info_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    auto sync_res = co_await stm_->sync(10s);
    if (!sync_res.has_value()) {
        co_return rpc::get_compaction_info_reply{
          .ec = convert_stm_errc(sync_res.error()),
        };
    }

    co_return co_await do_get_compaction_info(std::move(req));
}

ss::future<rpc::get_term_for_offset_reply>
db_domain_manager::get_term_for_offset(rpc::get_term_for_offset_request) {
    // TODO: Implement using state_reader
    // Use get_max_term to find term <= requested offset

    co_return rpc::get_term_for_offset_reply{
      .ec = rpc::errc::missing_ntp, // Placeholder
    };
}

ss::future<rpc::get_end_offset_for_term_reply>
db_domain_manager::get_end_offset_for_term(
  rpc::get_end_offset_for_term_request) {
    // TODO: Implement using state_reader
    // Find the last offset for the given term

    co_return rpc::get_end_offset_for_term_reply{
      .ec = rpc::errc::missing_ntp, // Placeholder
    };
}

ss::future<rpc::set_start_offset_reply>
db_domain_manager::set_start_offset(rpc::set_start_offset_request) {
    // TODO: Implement using db_update
    // Build set_start_offset_db_update, replicate, apply

    co_return rpc::set_start_offset_reply{
      .ec = rpc::errc::concurrent_requests, // Placeholder
    };
}

ss::future<rpc::remove_topics_reply>
db_domain_manager::remove_topics(rpc::remove_topics_request) {
    // TODO: Implement using db_update
    // Build remove_topics_db_update, replicate, apply

    co_return rpc::remove_topics_reply{
      .ec = rpc::errc::concurrent_requests, // Placeholder
      .not_removed = {},
    };
}

ss::future<rpc::get_compaction_infos_reply>
db_domain_manager::get_compaction_infos(rpc::get_compaction_infos_request req) {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return rpc::get_compaction_infos_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    chunked_hash_map<model::topic_id_partition, rpc::get_compaction_info_reply>
      compaction_infos;
    for (auto& log_req : req.logs) {
        auto log_info = co_await do_get_compaction_info(log_req);
        compaction_infos.insert_or_assign(log_req.tp, std::move(log_info));
    }

    co_return rpc::get_compaction_infos_reply{
      .responses = std::move(compaction_infos)};
}

ss::lowres_clock::duration db_domain_manager::gc_interval() const {
    return config::shard_local_cfg()
      .cloud_topics_long_term_garbage_collection_interval();
}

ss::future<std::expected<ss::rwlock::holder, db_domain_manager::errc>>
db_domain_manager::unique_db_lock() {
    auto fut = co_await ss::coroutine::as_future(
      db_instance_lock_.hold_write_lock());
    if (fut.failed()) {
        // XXX: map this type
        co_return std::unexpected(errc::db_error);
    }
    co_return std::move(fut.get());
}

ss::future<std::expected<ssx::checkpoint_mutex_units, db_domain_manager::errc>>
db_domain_manager::writer_lock() {
    auto fut = co_await ss::coroutine::as_future(writer_lock_.get_units());
    if (fut.failed()) {
        // XXX: map this type
        co_return std::unexpected(errc::db_error);
    }
    co_return std::move(fut.get());
}

ss::future<std::expected<ss::rwlock::holder, db_domain_manager::errc>>
db_domain_manager::shared_db_lock() {
    auto fut = co_await ss::coroutine::as_future(
      db_instance_lock_.hold_read_lock());
    if (fut.failed()) {
        // XXX: map this type
        co_return std::unexpected(errc::db_error);
    }
    co_return std::move(fut.get());
}

ss::future<std::expected<void, db_domain_manager::errc>>
db_domain_manager::maybe_init_db() {
    if (db_ && !db_->needs_reopen()) {
        co_return std::expected<void, errc>{};
    }

    auto wlock_res = co_await unique_db_lock();
    if (!wlock_res.has_value()) {
        co_return std::unexpected(wlock_res.error());
    }
    if (db_) {
        if (!db_->needs_reopen()) {
            co_return std::expected<void, errc>{};
        }
        auto close_res = co_await db_->close();
        if (!close_res.has_value()) {
            co_return std::unexpected(errc::db_error);
        }
        db_.reset();
        // Fallthrough to reopen.
    }
    auto db_res = co_await replicated_database::open(
      stm_.get(), staging_dir_, remote, bucket, as_);
    if (!db_res.has_value()) {
        co_return std::unexpected(errc::db_error);
    }
    db_ = std::move(db_res.value());
    co_return std::expected<void, errc>{};
}

ss::future<> db_domain_manager::gc_loop() {
    auto gate = maybe_gate();
    if (!gate.has_value()) {
        co_return;
    }

    // TODO: Implement GC using replicated_database
    // Similar to domain_manager but work with LSM database
    // garbage_collector gc(stm_.get(), object_io_);

    while (!as_.abort_requested()) {
        vlog(cd_log.debug, "Running garbage collection now...");

        // TODO: Run GC

        auto sleep_interval = gc_interval();
        vlog(
          cd_log.debug,
          "Re-running garbage collection in {}...",
          sleep_interval);
        auto sleep_res = co_await ss::coroutine::as_future(
          ssx::sleep_abortable(sleep_interval, as_));
        if (sleep_res.failed()) {
            auto eptr = sleep_res.get_exception();
            auto log_lvl = ssx::is_shutdown_exception(eptr)
                             ? ss::log_level::debug
                             : ss::log_level::warn;
            vlogl(
              cd_log,
              log_lvl,
              "Garbage collection loop hit exception while sleeping: {}",
              eptr);
        }
    }
    vlog(cd_log.debug, "Garbage collection loop stopped...");
}

ss::future<rpc::restore_domain_reply>
db_domain_manager::restore_domain(rpc::restore_domain_request req) {
    auto gate_res = maybe_gate();
    if (!gate_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    auto init_db_res = co_await maybe_init_db();
    if (!init_db_res.has_value()) {
        // XXX: different code? Likely shutting down but unclear.
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    if (db_->get_domain_uuid() == req.new_uuid) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::ok,
        };
    }

    auto lock_res = co_await unique_db_lock();
    if (!lock_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }

    cloud_storage_clients::object_key domain_prefix{
      fmt::format("{}", req.new_uuid)};
    auto meta_persist = co_await lsm::io::open_cloud_metadata_persistence(
      remote, bucket, domain_prefix);
    // When reading the manifest this will find the latest manifest at or below
    // the given epoch. So to find the latest, supply the max epoch.
    auto manifest_res = co_await meta_persist->read_manifest(
      lsm::internal::database_epoch::max());
    iobuf manifest_buf;
    std::optional<lsm::proto::manifest> manifest;
    if (manifest_res.has_value()) {
        manifest_buf = std::move(manifest_res.value());
        manifest = co_await lsm::proto::manifest::from_proto(
          manifest_buf.copy());
    }
    // XXX: change to use val from manifest
    auto reset_res = co_await db_->reset(req.new_uuid, std::move(manifest));
    if (!reset_res.has_value()) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    co_await db_->close();
    db_.reset();
    lock_res->return_all();

    init_db_res = co_await maybe_init_db();
    if (!init_db_res.has_value()) {
        // XXX: different code? Likely shutting down but unclear.
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::not_leader,
        };
    }
    if (db_->get_domain_uuid() != req.new_uuid) {
        co_return rpc::restore_domain_reply{
          .ec = rpc::errc::concurrent_requests,
        };
    }
    co_return rpc::restore_domain_reply{
      .ec = rpc::errc::ok,
    };
}

} // namespace cloud_topics::l1
