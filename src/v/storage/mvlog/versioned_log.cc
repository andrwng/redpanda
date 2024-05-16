// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/mvlog/versioned_log.h"

#include "container/fragmented_vector.h"
#include "model/fundamental.h"
#include "model/offset_interval.h"
#include "storage/mvlog/entry.h"
#include "storage/mvlog/file.h"
#include "storage/mvlog/log_reader.h"
#include "storage/mvlog/logger.h"
#include "storage/mvlog/readable_segment.h"
#include "storage/mvlog/segment_appender.h"
#include "storage/mvlog/segment_reader.h"
#include "storage/mvlog/version_id.h"
#include "storage/types.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

using namespace std::literals::chrono_literals;

namespace storage::experimental::mvlog {

active_segment::active_segment(
  std::unique_ptr<file> f, model::offset o, segment_id id, size_t target_size)
  : segment_file(std::move(f))
  , appender(std::make_unique<segment_appender>(segment_file.get()))
  , readable_seg(std::make_unique<readable_segment>(segment_file.get()))
  , construct_time(ss::lowres_clock::now())
  , target_max_size(target_size)
  , id(id)
  , base_offset(o)
  , next_offset(o) {}

active_segment::~active_segment() = default;

readonly_segment::~readonly_segment() = default;
readonly_segment::readonly_segment(std::unique_ptr<active_segment> active_seg)
  : segment_file(std::move(active_seg->segment_file))
  , readable_seg(std::move(active_seg->readable_seg))
  , id(active_seg->id)
  , offsets(model::bounded_offset_interval::checked(
      active_seg->base_offset, model::prev_offset(active_seg->next_offset))) {}

versioned_log::versioned_log(storage::ntp_config cfg)
  : ntp_cfg_(std::move(cfg)) {}

ss::future<> versioned_log::roll_for_tests() {
    auto lock = active_segment_lock_.try_get_units();
    if (!lock.has_value()) {
        lock = co_await active_segment_lock_.get_units();
    }
    if (active_seg_ == nullptr) {
        co_return;
    }
    co_await roll_unlocked();
}

ss::future<> versioned_log::roll_unlocked() {
    vassert(
      !active_segment_lock_.ready(),
      "roll_unlocked() must be called with active segment lock held");
    vassert(active_seg_ != nullptr, "Expected an active segment");
    vlog(
      log.info,
      "Rolling segment file {}",
      active_seg_->segment_file->filepath().c_str());
    co_await active_seg_->segment_file->flush();
    auto ro_seg = std::make_unique<readonly_segment>(std::move(active_seg_));
    segs_.emplace_back(std::move(ro_seg));
}

ss::future<> versioned_log::create_unlocked(model::offset base) {
    vassert(
      !active_segment_lock_.ready(),
      "create_unlocked() must be called with active segment lock held");
    vassert(active_seg_ == nullptr, "Expected no active segment");
    auto new_path = fmt::format(
      "{}/{}.log", ntp_cfg_.base_directory(), next_segment_id_());
    vlog(log.info, "Creating new segment file {}", new_path);
    auto f = co_await file_mgr_.create_file(std::filesystem::path{new_path});
    auto active_seg = std::make_unique<active_segment>(
      std::move(f), base, next_segment_id_, compute_max_segment_size());
    active_seg_ = std::move(active_seg);
    ++next_segment_id_;
}

ss::future<> versioned_log::apply_segment_ms() {
    auto lock = active_segment_lock_.try_get_units();
    if (!lock.has_value()) {
        lock = co_await active_segment_lock_.get_units();
    }
    if (active_seg_ == nullptr) {
        co_return;
    }
    const auto target_roll_deadline = compute_roll_deadline();
    if (!target_roll_deadline.has_value()) {
        co_return;
    }
    const auto now = ss::lowres_clock::now();
    if (now >= target_roll_deadline.value()) {
        vlog(
          log.debug,
          "Starting to roll {}, now vs deadline: {} vs {}",
          ss::sstring(active_seg_->segment_file->filepath()),
          now.time_since_epoch(),
          target_roll_deadline->time_since_epoch());
        co_await roll_unlocked();
    }
}

ss::future<> versioned_log::close() {
    // TODO(awong): handle concurrency. Should also prevent further appends,
    // readers, etc.
    if (active_seg_) {
        co_await active_seg_->segment_file->close();
    }
    for (auto& seg : segs_) {
        co_await seg->segment_file->close();
    }
    for (auto& seg : segs_pending_removal_) {
        co_await seg->segment_file->close();
    }
}

versioned_log::segments_t::iterator
versioned_log::find_seg_contains_or_greater(model::offset target) {
    // Want to find the first segment that contains `target` if it exists, or
    // the first segment after `target` if it doesn't.

    // TODO(awong): binary search with lower_bound.
    auto iter = segs_.begin();
    for (; iter != segs_.end(); iter++) {
        const auto& s = iter->get();
        if (s->offsets.max() <= target) {
            continue;
        }
        if (s->offsets.min() >= target) {
            return iter;
        }
    }
    return iter;
}

ss::future<> versioned_log::truncate_active_seg_unlocked(
  model::offset new_next_offset, version_id new_version_id) {
    vassert(active_seg_, "Expected active segment");
    vlog(
      log.debug,
      "Truncating active segment to offset {}: current offsets [{}, {})",
      new_next_offset,
      active_seg_->base_offset,
      active_seg_->next_offset);
    auto seg_truncation = co_await build_segment_truncation(
      new_next_offset, active_seg_.get());
    vlog(
      log.trace,
      "Gap at filepos [{}, {})",
      seg_truncation.gap.start_pos,
      seg_truncation.gap.start_pos + seg_truncation.gap.length);

    truncation_entry_body truncation_entry;
    truncation_entry.base_offset = new_next_offset;
    truncation_entry.gaps.emplace_back(seg_truncation);
    co_await active_seg_->appender->truncate(std::move(truncation_entry));

    // Update the in-memory state.
    active_seg_->readable_seg->mutable_gaps()->add(
      seg_truncation.gap, new_version_id);
    active_seg_->next_offset = new_next_offset;
    cur_version_id_ = new_version_id;
}

ss::future<> versioned_log::truncate(model::offset new_next_offset) {
    auto lock = active_segment_lock_.try_get_units();
    if (!lock.has_value()) {
        lock = co_await active_segment_lock_.get_units();
    }
    vlog(log.debug, "Truncating log so next offset is {}", new_next_offset);
    const auto log_next_offset = next_offset();
    const auto new_version_id = version_id(cur_version_id_() + 1);
    if (log_next_offset <= new_next_offset) {
        // Nothing to truncate!
        // TODO(awong): we should move the offset.
        vlog(
          log.debug,
          "Nothing to truncate: {} <= {}",
          log_next_offset,
          new_next_offset);
        co_return;
    }

    // Handle truncations that may entirely be covered by the active segment.
    if (active_seg_ && active_seg_->base_offset <= new_next_offset) {
        co_return co_await truncate_active_seg_unlocked(
          new_next_offset, new_version_id);
    }

    auto first_truncated_seg_iter = find_seg_contains_or_greater(
      new_next_offset);
    if (first_truncated_seg_iter == segs_.end()) {
        // The truncation point is in between the active segment and the last
        // segment. Still need to truncate the active segment.
        co_return co_await truncate_active_seg_unlocked(
          new_next_offset, new_version_id);
    }

    // From here on out, we assume that we will truncate at least one segment
    // below the active segment.
    const auto first_truncated_offsets
      = first_truncated_seg_iter->get()->offsets;
    // Guaranteed by find_seg_contains_or_greater().
    vassert(
      new_next_offset <= first_truncated_offsets.max(),
      "Found incorrect segment, {} is above {}",
      new_next_offset,
      first_truncated_offsets);

    truncation_entry_body truncation_entry;
    truncation_entry.base_offset = new_next_offset;
    std::optional<file_gap> first_partial_seg_gap;
    std::optional<file_gap> active_seg_gap;
    auto* first_truncated_seg = first_truncated_seg_iter->get();
    auto first_fully_truncated_seg_iter = first_truncated_seg_iter;
    if (first_truncated_seg->offsets.min() < new_next_offset) {
        // The first truncated segment isn't being fully truncated. Build the
        // partial truncation and move onto gathering fully truncated segments.
        auto seg_truncation = co_await build_segment_truncation(
          new_next_offset, first_truncated_seg);
        first_partial_seg_gap = seg_truncation.gap;
        truncation_entry.gaps.emplace_back(seg_truncation);
        ++first_fully_truncated_seg_iter;
    }
    // Collect segments that are fully truncated and need removal.
    for (auto iter = first_fully_truncated_seg_iter; iter != segs_.end();
         ++iter) {
        auto seg_truncation = build_full_segment_truncation(iter->get());
        truncation_entry.gaps.emplace_back(seg_truncation);
    }
    // Guaranteed to be non-empty because first_truncated_seg_iter != end.
    vassert(
      !truncation_entry.gaps.empty(), "Constructed empty truncation entry");

    if (active_seg_) {
        auto seg_truncation = build_full_segment_truncation(active_seg_.get());
        active_seg_gap = seg_truncation.gap;
    } else {
        co_await create_unlocked(new_next_offset);
    }
    co_await active_seg_->appender->truncate(std::move(truncation_entry));

    // Now that the truncation has been persisted to disk, apply in-memory
    // metadata updates.
    if (first_partial_seg_gap.has_value()) {
        first_truncated_seg->readable_seg->mutable_gaps()->add(
          *first_partial_seg_gap, new_version_id);
        auto new_bounds = model::bounded_offset_interval::checked(
          first_truncated_seg->offsets.min(),
          model::prev_offset(new_next_offset));
        first_truncated_seg->offsets = new_bounds;
    }
    if (active_seg_gap.has_value()) {
        active_seg_->readable_seg->mutable_gaps()->add(
          *active_seg_gap, new_version_id);
    }
    for (auto it = first_fully_truncated_seg_iter; it != segs_.end(); ++it) {
        vlog(
          log.debug,
          "Removing fully truncated segment {} offsets {}",
          it->get()->id,
          it->get()->offsets);
        segs_pending_removal_.emplace_back(it->release());
    }
    segs_.erase(first_fully_truncated_seg_iter, segs_.end());
    active_seg_->base_offset = new_next_offset;
    active_seg_->next_offset = new_next_offset;
    cur_version_id_ = new_version_id;
}

ss::future<> versioned_log::append(model::record_batch b) {
    auto lock = active_segment_lock_.try_get_units();
    if (!lock.has_value()) {
        lock = co_await active_segment_lock_.get_units();
    }
    if (active_seg_ != nullptr) {
        const auto cur_size = active_seg_->segment_file->size();
        const auto target_max_size = active_seg_->target_max_size;
        if (cur_size >= target_max_size) {
            vlog(
              log.debug,
              "Starting to roll {}, cur bytes vs target bytes: {} vs {}",
              active_seg_->segment_file->filepath().c_str(),
              cur_size,
              target_max_size);
            // The active segment needs to be rolled.
            co_await roll_unlocked();
        }
    }
    if (active_seg_ == nullptr) {
        co_await create_unlocked(b.base_offset());
    }
    vassert(active_seg_ != nullptr, "Expected active segment");
    auto next = model::next_offset(b.last_offset());
    co_await active_seg_->appender->append(std::move(b));
    active_seg_->next_offset = next;
}

model::record_batch_reader
versioned_log::make_reader(const log_reader_config& cfg) {
    auto read_start_offset = cfg.start_offset;
    auto read_max_offset = cfg.max_offset;
    ss::chunked_fifo<std::unique_ptr<segment_reader>> seg_readers;
    for (const auto& seg : segs_) {
        if (seg->offsets.max() < read_start_offset) {
            // The segment is entirely below the start.
            continue;
        }
        if (seg->offsets.min() > read_max_offset) {
            // The segment is entirely above the end of the read range.
            break;
        }
        seg_readers.emplace_back(seg->readable_seg->make_reader(version_id{0}));
    }
    // Add the active segment at the end if it falls in the read range.
    if (active_seg_ && active_seg_->base_offset != active_seg_->next_offset) {
        auto seg_range = model::bounded_offset_interval::unchecked(
          active_seg_->base_offset,
          model::prev_offset(active_seg_->next_offset));
        auto reader_range = model::bounded_offset_interval::unchecked(
          cfg.start_offset, cfg.max_offset);
        if (seg_range.overlaps(reader_range)) {
            seg_readers.emplace_back(
              active_seg_->readable_seg->make_reader(version_id{0}));
        }
    }
    return model::make_record_batch_reader<log_reader>(
      cfg, version_id{0}, std::move(seg_readers));
}

size_t versioned_log::segment_count() const {
    return segs_.size() + (active_seg_ ? 1 : 0);
}
bool versioned_log::has_active_segment() const {
    return active_seg_ != nullptr;
}
model::offset versioned_log::next_offset() const {
    if (active_seg_) {
        return active_seg_->next_offset;
    }
    if (!segs_.empty()) {
        return model::next_offset(segs_.back()->offsets.max());
    }
    // TODO(awong): Need to take into account log start offset.
    return model::offset{0};
}

size_t versioned_log::compute_max_segment_size() const {
    if (
      ntp_cfg_.has_overrides()
      && ntp_cfg_.get_overrides().segment_size.has_value()) {
        return ntp_cfg_.get_overrides().segment_size.value();
    }
    // TODO(awong): get defaults from log_manager
    // TODO(awong): clamp by min/max configs
    vassert(false, "Not implemented");
}

std::optional<ss::lowres_clock::time_point>
versioned_log::compute_roll_deadline() const {
    if (active_seg_ == nullptr || !ntp_cfg_.segment_ms().has_value()) {
        return std::nullopt;
    }
    return active_seg_->construct_time + ntp_cfg_.segment_ms().value();
}

ss::future<segment_truncation> versioned_log::build_segment_truncation(
  segment_id id,
  readable_segment& readable_seg,
  file& seg_file,
  model::offset truncate_offset) {
    version_id cur_version_id{0};
    auto seg_reader = readable_seg.make_reader(cur_version_id);
    auto find_res = co_await seg_reader->find_filepos(truncate_offset);
    if (find_res.has_error()) {
        // Likely an io error or corruption.
        // XXX(awong)
    }
    auto truncate_start_pos = find_res.value();
    if (!truncate_start_pos.has_value()) {
        // All data in the segment is below the truncation point.
        // This segment should not be truncated.
        // XXX(awong)
    }
    const auto truncate_end_pos = seg_file.size();

    // Return the segment truncation metadata.
    segment_truncation seg_truncation(
      id, *truncate_start_pos, truncate_end_pos - *truncate_start_pos);
    co_return seg_truncation;
}

segment_truncation
versioned_log::build_full_segment_truncation(active_segment* s) {
    segment_truncation seg_truncation(s->id, 0, s->segment_file->size());
    return seg_truncation;
}

segment_truncation
versioned_log::build_full_segment_truncation(readonly_segment* s) {
    segment_truncation seg_truncation(s->id, 0, s->segment_file->size());
    return seg_truncation;
}

} // namespace storage::experimental::mvlog
