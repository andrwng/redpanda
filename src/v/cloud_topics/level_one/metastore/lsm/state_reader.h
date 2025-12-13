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
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"

namespace cloud_topics::l1 {

struct extent_key_range {
    ss::sstring base_key;
    ss::sstring last_key;
};

// Encapsulate queries that operate on state in a database.
class state_reader {
public:
    enum class errc {
        io_error,
        corruption,
        shutting_down,
    };

    explicit state_reader(lsm::database& db)
      : db_(db) {}

    ss::future<std::expected<std::optional<metadata_row_value>, errc>>
    get_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<compaction_state>, errc>>
    get_compaction_metadata(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<object_entry>, errc>>
      get_object(object_id);

    ss::future<std::expected<std::optional<term_start>, errc>>
    get_max_term(const model::topic_id_partition&);

    ss::future<std::expected<std::optional<extent>, errc>>
    get_extent_ge(const model::topic_id_partition&, kafka::offset);

    ss::future<std::expected<std::optional<extent_key_range>, errc>>
    get_extent_range(
      const model::topic_id_partition&, kafka::offset base, kafka::offset last);

private:
    template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
    ss::future<std::expected<std::optional<ValT>, errc>>
    get_val(KeyEncodeArgs...);

    template<typename KeyT>
    struct key_with_iter {
        KeyT key;
        lsm::iterator iter;
    };
    template<typename KeyT, typename... KeyEncodeOtherArgs>
    ss::future<std::expected<std::optional<key_with_iter<KeyT>>, errc>>
    get_tp_iter_ge(const model::topic_id_partition&, KeyEncodeOtherArgs...);

    lsm::snapshot snap_;
};

} // namespace cloud_topics::l1
