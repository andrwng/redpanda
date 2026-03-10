/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "container/chunked_hash_map.h"
#include "container/chunked_vector.h"

#include <seastar/core/future.hh>

namespace cloud_topics::l1 {

/// Higher-level query interface for debugging and inspection of the L1
/// metastore. Composes state_reader queries to provide summaries, full dumps,
/// and invariant checks.
class debug_reader {
public:
    using error = state_reader::error;

    explicit debug_reader(lsm::snapshot snap)
      : reader_(std::move(snap)) {}

    /// Returns all topic_id_partitions with metadata in the database.
    ss::future<std::expected<chunked_vector<model::topic_id_partition>, error>>
    get_all_partitions();

    struct partition_summary {
        model::topic_id_partition tp;
        metadata_row_value metadata;
        size_t extent_count{0};
        kafka::offset extent_min_offset{};
        kafka::offset extent_max_offset{};
        size_t total_extent_data_size{0};
        size_t term_count{0};
        model::term_id min_term{};
        model::term_id max_term{};
        kafka::offset min_term_start_offset{};
        kafka::offset max_term_start_offset{};
        bool has_compaction_state{false};
        size_t cleaned_range_count{0};
        size_t tombstone_range_count{0};
    };

    ss::future<std::expected<partition_summary, error>>
    get_partition_summary(const model::topic_id_partition&);

    struct partition_dump {
        model::topic_id_partition tp;
        metadata_row_value metadata;
        chunked_vector<extent> extents;
        chunked_vector<term_start> term_starts;
        std::optional<compaction_state> compaction;
    };

    ss::future<std::expected<partition_dump, error>>
    dump_partition(const model::topic_id_partition&);

    ss::future<
      std::expected<chunked_vector<std::pair<object_id, object_entry>>, error>>
    get_objects(const chunked_vector<object_id>&);

    struct invariant_violation {
        ss::sstring check_name;
        ss::sstring description;
    };

    /// Check structural invariants for a single partition.
    static chunked_vector<invariant_violation>
    check_invariants(const partition_dump&);

    /// Check object reference invariants.
    static chunked_vector<invariant_violation> check_object_references(
      const partition_dump&,
      const chunked_hash_map<object_id, object_entry>& known_objects);

private:
    state_reader reader_;
};

} // namespace cloud_topics::l1
