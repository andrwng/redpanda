// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/log_eviction_stm.h"

#include "bytes/iostream.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "utils/gate_guard.h"

#include <seastar/core/future-util.hh>

namespace cluster {

struct snapshot_data
  : serde::
      envelope<snapshot_data, serde::version<0>, serde::compat_version<0>> {
    model::offset effective_start_offset{};

    auto serde_fields() { return std::tie(effective_start_offset); }

    friend std::ostream& operator<<(std::ostream& os, const snapshot_data& d) {
        fmt::print(
          os, "{{ effective_start_offset: {} }}", d.effective_start_offset);
        return os;
    }
};

log_eviction_stm::log_eviction_stm(
  raft::consensus* raft,
  ss::logger& logger,
  ss::abort_source& as,
  storage::kvstore& kvstore)
  : persisted_stm("log_eviction_stm.snapshot", logger, raft, kvstore)
  , _logger(logger)
  , _as(as) {}

ss::future<> log_eviction_stm::start() {
    monitor_log_eviction();
    return persisted_stm::start();
}

ss::future<bool> log_eviction_stm::sync_effective_start() {
    /// Call this method to ensure followers have processed up until the most
    /// recent known version of the special batch. This is particularly useful
    /// to know if the start offset is up to date in the case leadership has
    /// recently changed for example.
    static const auto sync_effective_start_timeout = 5s;
    return sync(sync_effective_start_timeout);
}

model::offset log_eviction_stm::effective_start_offset() const {
    vassert(
      _effective_start_offset >= _raft->last_snapshot_index(),
      "effective start offset: {} below last snapshot index: {}",
      _effective_start_offset,
      _raft->last_snapshot_index());
    return model::next_offset(_effective_start_offset);
}

ss::future<std::error_code> log_eviction_stm::truncate(
  model::offset kafka_truncate_offset,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    /// Create the special prefix_truncate batch, it is a model::record_batch
    /// with exactly one record within it, the point at which to truncate
    storage::record_batch_builder builder(
      model::record_batch_type::prefix_truncate, model::offset(0));
    /// Everthing below the requested offset should be truncated, requested
    /// offset itself will be the new low_watermark (readable)
    const model::offset as_log_offset
      = _raft->get_offset_translator_state()->to_log_offset(
        kafka_truncate_offset);
    auto key = serde::to_iobuf(as_log_offset - model::offset{1});
    builder.add_raw_kv(std::move(key), iobuf());
    auto batch = std::move(builder).build();
    /// After command replication all that can be guaranteed is that the command
    /// was replicated
    vlog(
      _logger.debug,
      "Replicating prefix_truncate command, truncate_offset: {} current start "
      "offset: {}, current last offset: {}",
      kafka_truncate_offset,
      _raft->last_snapshot_index(),
      _raft->last_visible_index());
    auto result = co_await replicate_commands(std::move(batch), deadline, as);
    if (result) {
        co_return result;
    }
    try {
        /// The following will return when the logs start_offset has
        /// officially been incremented at or above the requested truncated
        /// offset on this node. No guarantees of data removal / availability
        /// can be made at or after this point.
        co_await _min_consumable_offset_monitor.wait(
          as_log_offset, deadline, as);
    } catch (const ss::timed_out_error&) {
        co_return errc::timeout;
    } catch (const ss::abort_requested_exception&) {
        co_return errc::shutting_down;
    }
    co_return errc::success;
}

ss::future<std::error_code> log_eviction_stm::replicate_commands(
  model::record_batch batch,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto fut = _raft->replicate(
      _raft->term(),
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    /// Execute the replicate command bound by timeout and cancellable via
    /// abort_source mechanism
    result<raft::replicate_result> result{{}};
    try {
        if (as) {
            result = co_await ssx::with_timeout_abortable(
              std::move(fut), deadline, *as);
        } else {
            result = co_await ss::with_timeout(deadline, std::move(fut));
        }
    } catch (const ss::timed_out_error&) {
        result = errc::timeout;
    }

    if (!result) {
        vlog(
          _logger.error,
          "Failed to replicate prefix_truncate command, reason: {}",
          result.error());
        co_return result.error();
    }
    co_return make_error_code(errc::success);
}

void log_eviction_stm::monitor_log_eviction() {
    ssx::spawn_with_gate(_gate, [this] {
        return ss::do_until(
          [this] { return _as.abort_requested(); },
          [this] {
              return _raft->monitor_log_eviction(_as)
                .then([this](model::offset last_evicted) {
                    const auto max_collectible_offset
                      = _raft->log().stm_manager()->max_collectible_offset();
                    const auto eviction_point = std::min(
                      last_evicted, max_collectible_offset);

                    /// No retry logic, only attempt to truncate as much as
                    /// possible
                    if (should_process_evict(last_evicted)) {
                        return do_process_log_eviction(eviction_metadata{
                          .truncate_at = eviction_point,
                          .nearest_segment_boundary = eviction_point,
                          .requested_eviction_offset = last_evicted});
                    }
                    return ss::now();
                })
                .handle_exception_type(
                  [](const ss::abort_requested_exception&) {
                      // ignore abort requested exception, shutting down
                  })
                .handle_exception_type([](const ss::gate_closed_exception&) {
                    // ignore gate closed exception, shutting down
                })
                .handle_exception([this](std::exception_ptr e) {
                    vlog(
                      _logger.info,
                      "Error handling log eviction - {}, ntp: {}",
                      e,
                      _raft->ntp());
                });
          });
    });
}

bool log_eviction_stm::should_process_evict(model::offset truncate_at) {
    /// 1. Desired eviction point is behind the raft snapshot
    if (truncate_at < _raft->last_snapshot_index()) {
        return false;
    }

    /// 2. Desired eviction point is behind the effective start offset but >
    ///    rafts last snapshot
    if (truncate_at < _effective_start_offset) {
        /// 2a. Start offset was bumped but raft snapshot never written perhaps
        /// due to crash, event must be processed
        if (_raft->last_snapshot_index() < _effective_start_offset) {
            return true;
        }
        return false;
    }

    /// 3. In all other cases desired eviction point is ahead of agreed upon
    /// start offset, proceed to evict.
    return true;
}

ss::future<> log_eviction_stm::process_raft_event(eviction_metadata event) {
    static const auto max_retries = 200;
    static const auto sleep_duration = 200ms;
    int16_t retries = max_retries;

    /// Retry logic exists to attempt to satisfy the users request to truncate
    /// up until a certain point.
    while (should_process_evict(event.truncate_at) && --retries > 0) {
        const auto max_collectible_offset
          = _raft->log().stm_manager()->max_collectible_offset();
        if (event.truncate_at > max_collectible_offset) {
            vlog(
              _logger.info,
              "Cannot prefix truncate due to ongoing processing on data "
              "below the requested new start offset {} for ntp {}, "
              "retrying... ",
              event.truncate_at,
              _raft->ntp());
        } else {
            try {
                _requested_eviction_offset = event.truncate_at;
                co_await do_process_log_eviction(event);
            } catch (const std::exception& ex) {
                vlog(
                  _logger.error,
                  "Exception encountered when handing eviction event: {} for "
                  "ntp: {}",
                  ex,
                  _raft->ntp());
            }
            break;
        }
        /// If unsuccessful due to any reason, sleep and retry until limit
        try {
            co_await ss::sleep_abortable(sleep_duration, _as);
        } catch (const ss::sleep_aborted&) {
            ;
        }
    }
    if (retries == 0) {
        vlog(
          _logger.info,
          "Failed to process eviction event: {} at boundary: {} gave up after "
          "{} retries - ntp: {}",
          event.truncate_at,
          event.nearest_segment_boundary,
          max_retries,
          _raft->ntp());
    }
}

ss::future<>
log_eviction_stm::do_process_log_eviction(eviction_metadata event) {
    auto units = _process_mutex.get_units();
    _requested_eviction_offset = event.requested_eviction_offset;
    vlog(
      _logger.debug,
      "Handling log deletion notification truncate_point: {} nearest_segment: "
      "{} last_applied: {} ntp: {}",
      event.truncate_at,
      event.nearest_segment_boundary,
      last_applied_offset(),
      _raft->ntp());

    co_await _raft->visible_offset_monitor().wait(
      event.truncate_at, model::no_timeout, _as);
    co_await _raft->refresh_commit_index();
    co_await _raft->log().stm_manager()->ensure_snapshot_exists(
      event.nearest_segment_boundary);

    /// First update the new offset then conditionally write the raft snapshot,
    /// only performing this if this would bump forward the last_snapshot_index.
    _effective_start_offset = event.truncate_at;
    if (event.nearest_segment_boundary > _raft->last_snapshot_index()) {
        co_await _raft->write_snapshot(
          raft::write_snapshot_cfg(event.nearest_segment_boundary, iobuf()));
    }

    /// Then write the log_eviction's snapshot. If a crash were to occur before
    /// writing the stm snapshot, reprocessing would occur at the previous
    /// snapshot and raft->write_snapshot would be called again (with same
    /// offset) but be a noop.
    co_await make_snapshot();
    _min_consumable_offset_monitor.notify(
      model::next_offset(_effective_start_offset));
}

ss::future<> log_eviction_stm::apply(model::record_batch batch) {
    if (batch.header().type != model::record_batch_type::prefix_truncate) {
        co_return;
    }
    /// record_batches of type ::prefix_truncate are always of size 1
    const auto truncate_offset = serde::from_iobuf<model::offset>(
      batch.copy_records().begin()->key().copy());

    /// User may choose any arbitrary offset to truncate at, calculate the
    /// nearest segment boundary, this will be the offset at invocation of
    /// write snapshot, all data below this offset will be deleted.
    eviction_metadata event{
      .truncate_at = truncate_offset,
      .nearest_segment_boundary = model::offset{},
      .requested_eviction_offset = truncate_offset};
    if (
      auto record_boundary = _raft->log().nearest_batch_boundary_offset(
        truncate_offset)) {
        vassert(
          *record_boundary <= truncate_offset,
          "Record boundary incorrectly calculated");
        event.nearest_segment_boundary = *record_boundary;
        co_await process_raft_event(event);
    } else {
        /// Fail to continue, caller will eventually timeout. This could occur
        /// in the cause handle_eviction() was invoked, an attempt to calculate
        /// a boundary on a batch index that is no longer in the log
        vlog(
          _logger.debug,
          "Failed to calculate batch boundry at: {}, ntp: {}",
          truncate_offset,
          _raft->ntp());
    }
}

ss::future<> log_eviction_stm::handle_eviction() {
    auto mutex_hold = _process_mutex.get_units();

    /// In the case there is a gap detected in the log, the only path forward is
    /// to read the raft snapshot and begin processing from the raft
    /// last_snapshot_index
    auto raft_snapshot = co_await _raft->open_snapshot();
    if (!raft_snapshot) {
        throw std::runtime_error{fmt_with_ctx(
          fmt::format,
          "encountered a gap in the raft log (last_applied: {}, log start "
          "offset: {}), but can't find the snapshot - ntp: {}",
          last_applied_offset(),
          _raft->start_offset(),
          _raft->ntp())};
    }

    auto last_snapshot_index = raft_snapshot->metadata.last_included_index;
    co_await raft_snapshot->close();
    _effective_start_offset = last_snapshot_index;
    set_next(model::next_offset(last_snapshot_index));
    co_await make_snapshot();
    vlog(
      _logger.info,
      "Handled log eviction new effective start offset: {}",
      model::next_offset(_effective_start_offset));
}

ss::future<>
log_eviction_stm::apply_snapshot(stm_snapshot_header header, iobuf&& data) {
    auto snapshot = serde::from_iobuf<snapshot_data>(std::move(data));
    vlog(
      _logger.info,
      "Applying snapshot {} at offset: {} for ntp: {}",
      snapshot,
      header.offset,
      _raft->ntp());

    _effective_start_offset = snapshot.effective_start_offset;
    _last_snapshot_offset = header.offset;
    _insync_offset = header.offset;
    return ss::now();
}

ss::future<stm_snapshot> log_eviction_stm::take_snapshot() {
    vlog(
      _logger.trace,
      "Taking snapshot at offset: {} for ntp: {}",
      last_applied_offset(),
      _raft->ntp());
    iobuf snap_data = serde::to_iobuf(
      snapshot_data{.effective_start_offset = _effective_start_offset});
    co_return stm_snapshot::create(
      0, last_applied_offset(), std::move(snap_data));
}

ss::future<> log_eviction_stm::ensure_snapshot_exists(model::offset) {
    /// This class drives eviction, therefore it manages its own snapshots
    return ss::now();
}

} // namespace cluster
