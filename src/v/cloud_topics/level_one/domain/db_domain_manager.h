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

#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/rpc_types.h"
#include "ssx/checkpoint_mutex.h"

#include <seastar/core/rwlock.hh>

namespace cloud_topics::l1 {
class io;

// LSM database-backed version of domain_manager.
// Encapsulates management of a given L1 metastore domain using an LSM database.
// Expected to be running on the leader replicas of the partition that backs
// the STM.
//
// Key differences from domain_manager:
// 1. Uses replicated_database for persistent LSM storage
// 2. Queries use state_reader (async DB reads) instead of in-memory state
// 3. Updates use db_update types that generate write_batch_row operations
class db_domain_manager {
public:
    explicit db_domain_manager(ss::shared_ptr<stm> stm);

    void start();
    ss::future<> stop_and_wait();

    ss::future<rpc::add_objects_reply> add_objects(rpc::add_objects_request);

    ss::future<rpc::replace_objects_reply>
      replace_objects(rpc::replace_objects_request);

    ss::future<rpc::get_first_offset_ge_reply>
      get_first_offset_ge(rpc::get_first_offset_ge_request);

    ss::future<rpc::get_first_timestamp_ge_reply>
      get_first_timestamp_ge(rpc::get_first_timestamp_ge_request);

    ss::future<rpc::get_first_offset_for_bytes_reply>
      get_first_offset_for_bytes(rpc::get_first_offset_for_bytes_request);

    ss::future<rpc::get_offsets_reply> get_offsets(rpc::get_offsets_request);

    ss::future<rpc::get_compaction_info_reply>
      get_compaction_info(rpc::get_compaction_info_request);

    ss::future<rpc::get_term_for_offset_reply>
      get_term_for_offset(rpc::get_term_for_offset_request);

    ss::future<rpc::get_end_offset_for_term_reply>
      get_end_offset_for_term(rpc::get_end_offset_for_term_request);

    ss::future<rpc::set_start_offset_reply>
      set_start_offset(rpc::set_start_offset_request);

    ss::future<rpc::remove_topics_reply>
      remove_topics(rpc::remove_topics_request);

    ss::future<rpc::get_compaction_infos_reply>
      get_compaction_infos(rpc::get_compaction_infos_request);

private:
    enum class errc {
        db_error,
        request_rejected,
        shutting_down,
    };
    // Initializes the underlying database.
    // Once called, callers should expected that db_ is set.
    ss::future<std::expected<void, errc>> maybe_init_db();

    ss::future<std::expected<ss::rwlock::holder, errc>> shared_db_lock();
    ss::future<std::expected<ss::rwlock::holder, errc>> unique_db_lock();
    ss::future<std::expected<ssx::checkpoint_mutex_units, errc>> writer_lock();

    std::optional<ss::gate::holder> maybe_gate();
    ss::future<> gc_loop();
    ss::lowres_clock::duration gc_interval() const;

    ss::future<rpc::get_compaction_info_reply>
      do_get_compaction_info(rpc::get_compaction_info_request);

    ss::gate gate_;
    ss::abort_source as_;
    std::filesystem::path staging_dir_;
    cloud_io::remote* remote;
    cloud_storage_clients::bucket_name bucket;

    ss::shared_ptr<stm> stm_;

    // Hold in write mode when changing the db instance.
    // Hold in read mode for other access to the db that doesn't reopen the db.
    ss::rwlock db_instance_lock_;

    // Lock taken to serialize updates to the database, to ensure invariants
    // are checked and writes are applied atomically with respect to one
    // another. The db_instance_lock_ should be taken before taking this lock.
    //
    // TODO: make this more fine-grained, e.g. by doing per-partition locking;
    // note though that finer-grained locking will need to consider concurrent
    // updates to the same object entry from multiple partitions, so maybe
    // there'd need to be some form of object locking as well.
    ssx::checkpoint_mutex writer_lock_{"l1/domain/writer"};

    // Database backed by cloud IO and a replicated STM.
    // Valid when the underlying Raft partition is leader.
    std::unique_ptr<replicated_database> db_;
};

} // namespace cloud_topics::l1
