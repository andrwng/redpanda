/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/persisted_stm.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "raft/offset_monitor.h"
#include "seastarx.h"
#include "storage/types.h"
#include "utils/mutex.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>

namespace cluster {

class consensus;

/**
 * Responsible for taking snapshots triggered by underlying log segments
 * eviction.
 *
 * The process goes like this: storage layer will send a "deletion notification"
 * - a request to evict log up to a certain offset. log_eviction_stm will then
 * adjust that offset with _stm_manager->max_collectible_offset(), write the
 * raft snapshot and notify the storage layer that log eviction can safely
 * proceed up to the adjusted offset.
 *
 * This class also initiates and responds to delete-records events. Call
 * truncate() pushes a special prefix_truncate batch onto the log for which this
 * stm will be searching for. Upon processing of this record a new snapshot will
 * be written which may also trigger deletion of data on disk.
 */
class log_eviction_stm final
  : public persisted_stm<kvstore_backed_stm_snapshot> {
public:
    log_eviction_stm(
      raft::consensus*, ss::logger&, ss::abort_source&, storage::kvstore&);

    ss::future<> start() override;

    /// Truncate local log
    ///
    /// This method doesn't immediately delete the entries of the log below the
    /// requested offset but pushes a special record batch onto the log which
    /// when read by brokers, will invoke a routine that will perform the
    /// deletion
    ss::future<std::error_code> truncate(
      model::offset kafka_offset,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as
      = std::nullopt);

    /// Return the offset up to which the storage layer would like to
    /// prefix truncate the log, if any.
    std::optional<model::offset> eviction_requested_offset() const {
        if (_requested_eviction_offset == model::offset{}) {
            return std::nullopt;
        } else {
            return _requested_eviction_offset;
        }
    }

    /// This class drives eviction, therefore it manages its own snapshots
    ///
    /// Override to ensure it never unnecessarily waits
    ss::future<> ensure_snapshot_exists(model::offset) override;

    /// The actual start offset of the log with the delta factored in
    model::offset effective_start_offset() const;

    /// Ensure followers have processed up until the most recent known version
    /// of the batch representing the start offset
    ss::future<bool> sync_effective_start();

protected:
    ss::future<> apply_snapshot(stm_snapshot_header, iobuf&&) override;

    ss::future<stm_snapshot> take_snapshot() override;

private:
    struct eviction_metadata {
        /// Desired truncation point, will become new start offset. Some data
        /// below this offset will still be on disk, however inaccessable to
        /// clients.
        model::offset truncate_at{};
        /// Closest allowable offset to perform truncation at, all data below
        /// this offset will be deleted
        model::offset nearest_segment_boundary{};
        /// For storage events the offset that was requested to be evicted
        model::offset requested_eviction_offset;
    };

    void monitor_log_eviction();

    bool should_process_evict(model::offset);

    ss::future<> process_raft_event(eviction_metadata);
    ss::future<> do_process_log_eviction(eviction_metadata);

    ss::future<> do_write_snapshot(eviction_metadata event);
    ss::future<> apply(model::record_batch) override;
    ss::future<> handle_eviction() override;

    ss::future<std::error_code> replicate_commands(
      model::record_batch batch,
      ss::lowres_clock::time_point deadline,
      std::optional<std::reference_wrapper<ss::abort_source>> as);

private:
    ss::logger& _logger;
    ss::abort_source& _as;
    model::offset _requested_eviction_offset;
    model::offset _effective_start_offset;

    mutex _process_mutex;
    raft::offset_monitor _min_consumable_offset_monitor;
};

} // namespace cluster
