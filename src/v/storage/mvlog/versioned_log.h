// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "container/fragmented_vector.h"
#include "model/offset_interval.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/segment_identifier.h"
#include "storage/mvlog/version_id.h"
#include "storage/ntp_config.h"
#include "utils/mutex.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

namespace storage {
struct log_reader_config;
struct truncate_config;
} // namespace storage

namespace storage::experimental::mvlog {

class readable_segment;
class segment_appender;

struct active_segment {
    active_segment(active_segment&) = delete;
    active_segment(active_segment&&) = delete;
    active_segment& operator=(active_segment&) = delete;
    active_segment& operator=(active_segment&&) = delete;

    active_segment(
      std::unique_ptr<file>, model::offset, segment_id, size_t target_size);
    ~active_segment();

    std::unique_ptr<file> segment_file;
    std::unique_ptr<segment_appender> appender;
    std::unique_ptr<readable_segment> readable_seg;

    // Time this segment was created, used to determine the age of the segment
    // when applying segment.ms.
    const ss::lowres_clock::time_point construct_time;

    // Target max size of the segment.
    const size_t target_max_size;

    const segment_id id;
    model::offset base_offset;
    model::offset next_offset;
};

struct readonly_segment {
    readonly_segment(readonly_segment&) = delete;
    readonly_segment(readonly_segment&&) = delete;
    readonly_segment& operator=(readonly_segment&) = delete;
    readonly_segment& operator=(readonly_segment&&) = delete;

    explicit readonly_segment(std::unique_ptr<active_segment>);
    ~readonly_segment();

    std::unique_ptr<file> segment_file;
    std::unique_ptr<readable_segment> readable_seg;
    const segment_id id;
    model::bounded_offset_interval offsets;
};

class versioned_log {
public:
    versioned_log& operator=(versioned_log&) = delete;
    versioned_log& operator=(versioned_log&&) = delete;
    versioned_log(versioned_log&) = delete;
    versioned_log(versioned_log&&) = delete;
    ~versioned_log() = default;

    explicit versioned_log(storage::ntp_config cfg);

    // Closes the segments in the log.
    ss::future<> close();

    // Truncates the suffix of the log so that the next offset is the offset
    // specified in the config.
    ss::future<> truncate(model::offset new_next_offset);

    // Appends the record batch to the active segment, creating a new one if it
    // doesn't exist or if past the configured segment size. Upon returning,
    // the record batch will be visible to new readers, though may not be
    // persisted to disk.
    // TODO(awong): implement flushing.
    ss::future<> append(model::record_batch);

    // Checks whether the segment rolling deadline has passed for the active
    // segment, e.g. as specified by the segment.ms property. If so, rolls the
    // segment, leaving the log without an active segment.
    ss::future<> apply_segment_ms();

    model::record_batch_reader make_reader(const log_reader_config&);

    // Returns the total of segments in the log.
    size_t segment_count() const;

    // Returns whether or not the log has an active segment.
    bool has_active_segment() const;
    model::offset next_offset() const;

    // Rolls the active segment.
    ss::future<> roll_for_tests();

private:
    using segments_t = ss::circular_buffer<std::unique_ptr<readonly_segment>>;

    // Returns the appropriate target segment size for the log.
    size_t compute_max_segment_size() const;

    // Returns the point in time after which a segment should be rolled.
    std::optional<ss::lowres_clock::time_point> compute_roll_deadline() const;
    segments_t::iterator find_seg_contains_or_greater(model::offset);

    // Rolls the active segment, leaving the log without an active segment.
    // Must be called while the active segment lock is held and while there is
    // already an active segment.
    ss::future<> roll_unlocked();

    // Creates a new active segment.
    // Mutst be called while the active segment lock is held and while there is
    // no active segment.
    ss::future<> create_unlocked(model::offset base);

    ss::future<> truncate_active_seg_unlocked(
      model::offset new_next_offset, version_id new_version_id);

    ss::future<segment_truncation> build_segment_truncation(
      segment_id, readable_segment&, file&, model::offset truncate_offset);

    ss::future<segment_truncation>
    build_segment_truncation(model::offset o, active_segment* s) {
        co_return co_await build_segment_truncation(
          s->id, *s->readable_seg, *s->segment_file, o);
    }
    ss::future<segment_truncation>
    build_segment_truncation(model::offset o, readonly_segment* s) {
        co_return co_await build_segment_truncation(
          s->id, *s->readable_seg, *s->segment_file, o);
    }
    static segment_truncation build_full_segment_truncation(readonly_segment*);
    static segment_truncation build_full_segment_truncation(active_segment*);

    // NTP config with which to get segment properties.
    const ntp_config ntp_cfg_;

    // File manager to manage creation and removal of files.
    file_manager file_mgr_;

    // The id of the next segment to create.
    segment_id next_segment_id_{0};

    // The current version id. New versions are created during suffix
    // truncations, when we have to undo some appends.
    version_id cur_version_id_{0};

    // Lock protecting mutations that involve the active segment.
    mutex active_segment_lock_{"active_segment_lock"};

    // The segment containing the most recent data, that will be written to to
    // if an operation requires appending to the log.
    // May be null, e.g. if we haven't written for longer than segment.ms.
    std::unique_ptr<active_segment> active_seg_;

    // The segments with offsets lower than that in the active segment.
    // Ordered by offsets.
    segments_t segs_;

    // Segments that need to be removed.
    chunked_vector<std::unique_ptr<readonly_segment>> segs_pending_removal_;
};

} // namespace storage::experimental::mvlog
