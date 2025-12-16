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

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/state_update.h"

namespace cloud_topics::l1 {

class new_obj_builder {
public:
    new_obj_builder(object_id oid, size_t footer_pos, size_t object_size) {
        out.oid = oid;
        out.footer_pos = footer_pos;
        out.object_size = object_size;
    }
    new_obj_builder(const new_obj_builder&) = delete;
    new_obj_builder(new_obj_builder&&) = default;
    new_object build() { return std::move(out); }

    new_obj_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        auto tpr = model::topic_id_partition::from(tpr_str);
        out.extent_metas[tpr.topic_id][tpr.partition] = new_object::metadata{
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .filepos = first_pos,
          .len = last_pos - first_pos,
        };
        return *this;
    }

private:
    new_object out;
};

struct add_objects_builder {
public:
    add_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    add_objects_builder&
    add_term_start(std::string_view tp_str, model::term_id t, kafka::offset o) {
        auto tp = model::topic_id_partition::from(tp_str);
        out.new_terms[tp].emplace_back(
          term_start{.term_id = t, .start_offset = o});
        return *this;
    }
    add_objects_update build() { return std::move(out); }

private:
    add_objects_update out;
};

struct replace_objects_builder {
public:
    replace_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    replace_objects_builder& clean(
      std::string_view tp_str,
      compaction_state_update::cleaned_range r,
      model::timestamp cleaned_at) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.cleaned_at = cleaned_at;
        c_state.new_cleaned_ranges.push_back(std::move(r));
        return *this;
    }
    replace_objects_builder& clean_tombstones(
      std::string_view tp_str, kafka::offset base, kafka::offset last) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.removed_tombstones_ranges.insert(base, last);
        return *this;
    }
    replace_objects_update build() { return std::move(out); }

private:
    replace_objects_update out;
};

} // namespace cloud_topics::l1
