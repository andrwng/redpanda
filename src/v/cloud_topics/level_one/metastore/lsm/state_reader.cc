/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "lsm/core/exceptions.h"
#include "ssx/future-util.h"

#include <exception>

namespace cloud_topics::l1 {

namespace {

state_reader::errc to_errc(std::exception_ptr e) {
    if (ssx::is_shutdown_exception(e)) {
        return state_reader::errc::shutting_down;
    }
    try {
        std::rethrow_exception(e);
    } catch (lsm::abort_requested_exception&) {
        return state_reader::errc::shutting_down;
    } catch (lsm::corruption_exception&) {
        return state_reader::errc::corruption;
    } catch (...) {
        return state_reader::errc::io_error;
    }
}

model::topic_id_partition next_partition(const model::topic_id_partition& tp) {
    return model::topic_id_partition(
      tp.topic_id, model::partition_id(tp.partition() + 1));
}

} // namespace

ss::future<std::expected<std::optional<metadata_row_value>, state_reader::errc>>
state_reader::get_metadata(const model::topic_id_partition& tidp) {
    return get_val<metadata_row_key, metadata_row_value>(tidp);
}

ss::future<std::expected<std::optional<compaction_state>, state_reader::errc>>
state_reader::get_compaction_metadata(const model::topic_id_partition& tidp) {
    auto opt_val_res
      = co_await get_val<compaction_row_key, compaction_row_value>(tidp);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(opt_val_res.error());
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->state;
}

ss::future<std::expected<std::optional<object_entry>, state_reader::errc>>
state_reader::get_object(object_id oid) {
    auto opt_val_res = co_await get_val<object_row_key, object_row_value>(oid);
    if (!opt_val_res.has_value()) {
        co_return std::unexpected(opt_val_res.error());
    }
    if (!opt_val_res.value().has_value()) {
        co_return std::nullopt;
    }
    co_return opt_val_res.value()->object;
}

ss::future<std::expected<std::optional<term_start>, state_reader::errc>>
state_reader::get_max_term(const model::topic_id_partition& tidp) {
    iobuf val_buf;
    model::term_id term;
    try {
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(
          term_row_key::encode(next_partition(tidp), model::term_id(0)));
        if (!iter.valid()) {
            co_await iter.seek_to_last();
        } else {
            co_await iter.prev();
        }
        if (!iter.valid()) {
            co_return std::nullopt;
        }
        auto key = term_row_key::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        term = key->term;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<term_row_value>(std::move(val_buf));
        co_return term_start{
          .term_id = term, .start_offset = val.term_start_offset};
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}

ss::future<std::expected<std::optional<extent>, state_reader::errc>>
state_reader::get_extent_ge(
  const model::topic_id_partition& tidp, kafka::offset o) {
    iobuf val_buf;
    kafka::offset base_offset;
    try {
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(extent_row_key::encode(tidp, o));
        if (!iter.valid()) {
            co_return std::nullopt;
        }
        auto key = extent_row_key::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        base_offset = key->base_offset;
        val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<extent_row_value>(std::move(val_buf));
        co_return extent{
          .base_offset = base_offset,
          .last_offset = val.last_offset,
          .max_timestamp = val.max_timestamp,
          .filepos = val.filepos,
          .len = val.len,
          .oid = val.oid,
        };
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}

ss::future<std::expected<std::optional<extent_key_range>, state_reader::errc>>
state_reader::get_extent_range(
  const model::topic_id_partition& tidp,
  kafka::offset base,
  kafka::offset last) {
    ss::sstring base_key;
    ss::sstring last_key;
    iobuf last_val_buf;
    try {
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(extent_row_key::encode(tidp, base));
        if (!iter.valid()) {
            co_return std::nullopt;
        }
        auto key = extent_row_key::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp || key->base_offset != base) {
            // TODO: it's possible this isn't a key at all, in which case this
            // could be some flavor of corruption!
            co_return std::nullopt;
        }
        last_val_buf = iter.value();
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
    try {
        auto val = serde::from_iobuf<extent_row_value>(std::move(last_val_buf));
        auto key = extent_row_key::decode(iter.key());
        if (val.last_offset != last) {
            co_return std::nullopt;
        }
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
    co_return extent_key_range{
      .base_key = std::move(base_key),
      .last_key = std::move(last_key),
    };
}

template<typename KeyT, typename ValT, typename... KeyEncodeArgs>
ss::future<std::expected<std::optional<ValT>, state_reader::errc>>
state_reader::get_val(KeyEncodeArgs... args) {
    auto fut = co_await ss::coroutine::as_future(
      snap_.get(KeyT::encode(args...)));
    if (fut.failed()) {
        co_return std::unexpected(to_errc(fut.get_exception()));
    }
    auto opt_buf = fut.get();
    if (!opt_buf.has_value()) {
        co_return std::nullopt;
    }
    try {
        auto val = serde::from_iobuf<ValT>(std::move(*opt_buf));
        co_return val;
    } catch (...) {
        co_return std::unexpected(errc::corruption);
    }
}
} // namespace cloud_topics::l1

template<typename KeyT, typename... KeyEncodeOtherArgs>
ss::future<std::expected<
  std::optional<state_reader::key_with_iter<KeyT>>,
  state_reader::errc>>
state_reader::get_tp_iter_ge(
  const model::topic_id_partition& tidp, KeyEncodeOtherArgs... args) {
    try {
        auto iter = co_await snap_.create_iterator();
        co_await iter.seek(KeyT::encode(tidp, args...));
        if (!iter.valid()) {
            co_return std::nullopt;
        }
        auto key = KeyT::decode(iter.key());
        if (!key.has_value() || key->tidp != tidp) {
            co_return std::nullopt;
        }
        co_return key_with_iter<KeyT>{
          .key = std::move(*key),
          .iter = std::move(iter),
        };
    } catch (...) {
        co_return std::unexpected(to_errc(std::current_exception()));
    }
}

