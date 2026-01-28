/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"
#include "lsm/core/internal/files.h"
#include "lsm/core/internal/keys.h"
#include "lsm/db/table_cache.h"
#include "lsm/io/disk_persistence.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/sst/builder.h"
#include "test_utils/async.h"
#include "utils/uuid.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <filesystem>

namespace {

using lsm::internal::file_handle;
using lsm::internal::operator""_file_id;
using lsm::internal::operator""_db_epoch;
using lsm::internal::operator""_key;

class TableCacheTest : public testing::Test {
public:
    constexpr static size_t default_max_entries = 10;
    std::pair<lsm::internal::file_handle, size_t> make_sst() {
        auto filename = file_handle{.id = ++_latest_id, .epoch = 0_db_epoch};
        auto file = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(std::move(file), {});
        // Just make empty SST files - the cache doesn't care about the contents
        builder.finish().get();
        builder.close().get();
        return std::make_pair(filename, builder.file_size());
    }

    // Create an SST file with actual data that requires I/O to read
    std::pair<lsm::internal::file_handle, size_t> make_sst_with_data() {
        auto filename = file_handle{.id = ++_latest_id, .epoch = 0_db_epoch};
        auto file = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(std::move(file), {});
        // Add some key-value pairs
        builder.add("key1"_key, iobuf::from("value1")).get();
        builder.add("key2"_key, iobuf::from("value2")).get();
        builder.add("key3"_key, iobuf::from("value3")).get();
        builder.finish().get();
        builder.close().get();
        return std::make_pair(filename, builder.file_size());
    }

    lsm::db::table_cache
    make_table_cache(size_t max_entries = default_max_entries) {
        return {
          _persistence.get(),
          max_entries,
          ss::make_lw_shared<lsm::sst::block_cache>(1_MiB)};
    }

    void TearDown() override { _persistence->close().get(); }

private:
    lsm::internal::file_id _latest_id;
    std::unique_ptr<lsm::io::data_persistence> _persistence
      = lsm::io::make_memory_data_persistence();
};

// Test fixture that uses disk persistence to reproduce real I/O behavior
class TableCacheDiskTest : public testing::Test {
public:
    void SetUp() override {
        std::filesystem::path tmpdir = std::getenv("TEST_TMPDIR");
        auto subdir = ss::sstring(uuid_t::create());
        _persistence = lsm::io::open_disk_data_persistence(
                         tmpdir / std::string_view(subdir))
                         .get();
    }

    std::pair<lsm::internal::file_handle, size_t> make_sst_with_data() {
        auto filename = file_handle{.id = ++_latest_id, .epoch = 0_db_epoch};
        auto file = _persistence->open_sequential_writer(filename).get();
        lsm::sst::builder builder(std::move(file), {});
        // Add some key-value pairs
        builder.add("key1"_key, iobuf::from("value1")).get();
        builder.add("key2"_key, iobuf::from("value2")).get();
        builder.add("key3"_key, iobuf::from("value3")).get();
        builder.finish().get();
        builder.close().get();
        return std::make_pair(filename, builder.file_size());
    }

    lsm::db::table_cache make_table_cache(size_t max_entries = 10) {
        return {
          _persistence.get(),
          max_entries,
          ss::make_lw_shared<lsm::sst::block_cache>(1_MiB)};
    }

    void TearDown() override {
        if (_persistence) {
            _persistence->close().get();
        }
    }

private:
    lsm::internal::file_id _latest_id;
    std::unique_ptr<lsm::io::data_persistence> _persistence;
};

} // namespace

TEST_F(TableCacheTest, CanOpenFiles) {
    auto cache = make_table_cache();
    auto [id1, size1] = make_sst();
    auto it = cache.create_iterator(id1, size1).get();
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    it = nullptr;
    EXPECT_EQ(cache.statistics().open_file_handles, 1);
    cache.close().get();
}

TEST_F(TableCacheTest, ThrowsOnMissingFiles) {
    auto cache = make_table_cache();
    EXPECT_ANY_THROW(cache.create_iterator({.id = 999_file_id}, 10).get());
    EXPECT_EQ(cache.statistics().open_file_handles, 0);
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntries) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_handle, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    for (const auto& [h, size] : files) {
        cache.create_iterator(h, size).get();
    }
    tests::drain_task_queue().get();
    // We get 5 on the small queue (+1 over the limit) and a full ghost queue of
    // 2 entries. The main queue is empty because nothing is touched twice.
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}

TEST_F(TableCacheTest, MaxEntriesWithOpenIterators) {
    auto cache = make_table_cache();
    std::map<lsm::internal::file_handle, size_t> files;
    for (size_t i = 0; i < default_max_entries * 2; ++i) {
        files.insert(make_sst());
    }
    std::vector<std::unique_ptr<lsm::internal::iterator>> iters;
    iters.reserve(files.size());
    for (const auto& [h, size] : files) {
        iters.push_back(cache.create_iterator(h, size).get());
    }
    // We burst over because there are open iterators. We could consider instead
    // limiting new entries to be created past the limit when there are open
    // iterators, but for now we burst.
    EXPECT_EQ(cache.statistics().open_file_handles, 20) << cache.statistics();
    iters.clear();
    tests::drain_task_queue().get();
    // But once everything is cleaned up, we only have 7 things enqueued (for
    // why 7 see the comment inMaxEntries).
    EXPECT_EQ(cache.statistics().open_file_handles, 7) << cache.statistics();
    cache.close().get();
}

// This test reproduces a crash where evict() closes a reader while an iterator
// is still using it. The crash occurs because the iterator tries to read from
// a closed file.
//
// Uses disk persistence because memory persistence doesn't invalidate state
// on close the same way real disk files do.
TEST_F(TableCacheDiskTest, EvictWithActiveIteratorCrashes) {
    auto cache = make_table_cache();
    auto [handle, size] = make_sst_with_data();

    // Create an iterator - this holds a reference to the reader
    auto iter = cache.create_iterator(handle, size).get();

    // Evict the file while the iterator is still alive.
    // This will close the underlying reader.
    cache.evict(handle).get();

    // Now try to use the iterator - this should crash because the reader
    // has been closed but the iterator still tries to use it.
    // The crash manifests as a segfault when accessing the closed file's
    // memory_dma_alignment (offset 8 in the file object).
    iter->seek_to_first().get();

    // If we get here without crashing, the bug is fixed
    EXPECT_TRUE(iter->valid());
    cache.close().get();
}
