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

#include "cloud_topics/level_one/metastore/lsm/debug_reader.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "container/chunked_vector.h"

#include <cstddef>
#include <cstdint>
#include <string>

namespace cloud_topics::l1 {

// Summary information about a single SST file in the LSM tree.
struct lsm_file_info {
    uint64_t epoch;
    uint64_t id;
    uint64_t size_bytes;
    std::string smallest_key_info;
    std::string largest_key_info;
};

// Information about a single level in the LSM tree.
struct lsm_level_info {
    int32_t level_number;
    chunked_vector<lsm_file_info> files;
};

struct database_stats {
    size_t active_memtable_bytes{0};
    size_t immutable_memtable_bytes{0};
    size_t total_size_bytes{0};
    std::vector<lsm_level_info> levels;
};

// Abstract base class for domain managers.
// Defines the interface used by leader_router to interact with domain managers.
class domain_manager {
public:
    domain_manager() = default;
    domain_manager(const domain_manager&) = delete;
    domain_manager(domain_manager&&) = delete;
    domain_manager& operator=(const domain_manager&) = delete;
    domain_manager& operator=(domain_manager&&) = delete;
    virtual ~domain_manager() = default;

    virtual void start() = 0;
    virtual ss::future<> stop_and_wait() = 0;

    virtual ss::future<rpc::add_objects_reply>
      add_objects(rpc::add_objects_request) = 0;

    virtual ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request) = 0;

    virtual ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request) = 0;

    virtual ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request) = 0;

    virtual ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(rpc::get_first_offset_for_bytes_request) = 0;

    virtual ss::future<rpc::get_offsets_reply>
      get_offsets(rpc::get_offsets_request) = 0;

    virtual ss::future<rpc::get_size_reply> get_size(rpc::get_size_request) = 0;

    virtual ss::future<rpc::get_compaction_info_reply>
      get_compaction_info(rpc::get_compaction_info_request) = 0;

    virtual ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request) = 0;

    virtual ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request) = 0;

    virtual ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request) = 0;

    virtual ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request) = 0;

    virtual ss::future<rpc::get_compaction_infos_reply>
      get_compaction_infos(rpc::get_compaction_infos_request) = 0;

    virtual ss::future<rpc::get_extent_metadata_reply>
      get_extent_metadata(rpc::get_extent_metadata_request) = 0;

    virtual ss::future<rpc::flush_domain_reply>
      flush_domain(rpc::flush_domain_request) = 0;

    virtual ss::future<rpc::restore_domain_reply>
      restore_domain(rpc::restore_domain_request) = 0;

    virtual ss::future<std::expected<database_stats, rpc::errc>>
    get_database_stats() = 0;

    virtual ss::future<rpc::preregister_objects_reply>
      preregister_objects(rpc::preregister_objects_request) = 0;

    // Debug/inspection endpoints.

    virtual ss::future<
      std::expected<chunked_vector<debug_reader::partition_summary>, rpc::errc>>
    get_partition_summaries(
      chunked_vector<model::topic_id_partition> partitions)
      = 0;

    struct object_dump_entry {
        object_id oid;
        object_entry entry;
        enum class existence {
            unspecified,
            exists,
            missing,
            check_failed,
        };
        existence exists_in_cloud{existence::unspecified};
    };

    struct dump_result {
        chunked_vector<debug_reader::partition_dump> partitions;
        chunked_vector<object_dump_entry> objects;
    };

    virtual ss::future<std::expected<dump_result, rpc::errc>>
    dump_partition_state(
      chunked_vector<model::topic_id_partition> partitions,
      bool include_objects,
      bool check_object_existence)
      = 0;

    struct invariant_check_result {
        model::topic_id_partition tp;
        chunked_vector<debug_reader::invariant_violation> violations;
    };

    virtual ss::future<
      std::expected<chunked_vector<invariant_check_result>, rpc::errc>>
    check_partition_invariants(
      chunked_vector<model::topic_id_partition> partitions,
      bool check_object_existence)
      = 0;
};

} // namespace cloud_topics::l1
