/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"

#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "model/fundamental.h"
#include "serde/rw/iobuf.h"
#include "serde/rw/named_type.h"
#include "serde/rw/optional.h"
#include "serde/rw/uuid.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

// apply_write_batch_update implementation

std::expected<apply_write_batch_update, lsm_update_error>
apply_write_batch_update::build(
  const lsm_state& state, chunked_vector<write_batch_row> rows) {
    apply_write_batch_update update{
      .rows = std::move(rows),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, lsm_update_error>
apply_write_batch_update::can_apply(const lsm_state& state) const {
    if (state.domain_uuid != expected_uuid) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Expected domain UUID: {}, actual: {}",
            expected_uuid,
            state.domain_uuid)});
    }
    if (rows.empty()) {
        return std::unexpected(lsm_update_error{"Write batch is empty"});
    }
    return std::monostate{};
}

std::expected<std::monostate, lsm_update_error>
apply_write_batch_update::apply(lsm_state& state, model::offset base_offset) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    lsm::sequence_number seqno(base_offset() + state.seqno_delta);
    for (auto& row : rows) {
        state.volatile_buffer.push_back(
          volatile_row{.seqno = seqno, .row = std::move(row)});
    }

    return std::monostate{};
}

apply_write_batch_update apply_write_batch_update::copy() const {
    apply_write_batch_update ret;
    for (const auto& r : rows) {
        ret.rows.push_back(
          write_batch_row{
            .key = r.key,
            .value = r.value.copy(),
          });
    }
    return ret;
}

ss::future<> persist_manifest_update::serde_async_write(iobuf& buf) const {
    auto manifest_buf = co_await manifest.to_proto();
    serde::write(buf, std::move(manifest_buf));
}

ss::future<> persist_manifest_update::serde_async_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    auto manifest_buf = read_nested<iobuf>(in, h._bytes_left_limit);
    manifest = co_await lsm::proto::manifest::from_proto(
      std::move(manifest_buf));
}
std::expected<persist_manifest_update, lsm_update_error>
persist_manifest_update::build(
  const lsm_state& state,
  domain_uuid expected_uuid,
  lsm::proto::manifest manifest) {
    persist_manifest_update update{
      .expected_uuid = expected_uuid,
      .manifest = std::move(manifest),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, lsm_update_error>
persist_manifest_update::can_apply(const lsm_state& state) const {
    if (state.domain_uuid != expected_uuid) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Expected domain UUID: {}, actual: {}",
            expected_uuid,
            state.domain_uuid)});
    }
    if (!state.persisted_manifest.has_value()) {
        return std::monostate{};
    }
    if (
      manifest.get_last_seqno() > state.persisted_manifest->get_last_seqno()) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Cannot persist manifest up to seqno {} which is beyond current "
            "seqno {}",
            manifest.get_last_seqno(),
            state.persisted_manifest->get_last_seqno())});
    }
    if (
      manifest.get_database_epoch()
      > state.persisted_manifest->get_database_epoch()) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Cannot persist manifest up from epoch {} which is beyond current "
            "epoch {}",
            manifest.get_database_epoch(),
            state.persisted_manifest->get_database_epoch())});
    }

    return std::monostate{};
}

std::expected<std::monostate, lsm_update_error>
persist_manifest_update::apply(lsm_state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }

    while (!state.volatile_buffer.empty()
           && state.volatile_buffer.front().seqno
                <= manifest.get_last_seqno()) {
        state.volatile_buffer.pop_front();
    }
    state.persisted_manifest = std::move(manifest);
    return std::monostate{};
}

std::expected<set_domain_uuid_update, lsm_update_error>
set_domain_uuid_update::build(const lsm_state& state, domain_uuid uuid) {
    set_domain_uuid_update update{
      .uuid = uuid,
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, lsm_update_error>
set_domain_uuid_update::can_apply(const lsm_state& state) const {
    if (state.domain_uuid != domain_uuid{}) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Cannot set domain UUID: already set to {}", state.domain_uuid)});
    }
    return std::monostate{};
}

std::expected<std::monostate, lsm_update_error>
set_domain_uuid_update::apply(lsm_state& state) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }

    state.domain_uuid = uuid;

    return std::monostate{};
}

ss::future<> reset_manifest_update::serde_async_write(iobuf& buf) const {
    serde::write(buf, new_uuid);
    std::optional<iobuf> manifest_buf;
    if (new_manifest) {
        manifest_buf = co_await new_manifest->to_proto();
    }
    serde::write(buf, std::move(manifest_buf));
}

ss::future<> reset_manifest_update::serde_async_read(
  iobuf_parser& in, const serde::header& h) {
    using serde::read_nested;
    new_uuid = read_nested<decltype(new_uuid)>(in, h._bytes_left_limit);
    auto manifest_buf = read_nested<std::optional<iobuf>>(
      in, h._bytes_left_limit);
    if (manifest_buf.has_value()) {
        new_manifest = co_await lsm::proto::manifest::from_proto(
          std::move(*manifest_buf));
    }
}

std::expected<reset_manifest_update, lsm_update_error>
reset_manifest_update::build(
  const lsm_state& state,
  domain_uuid uuid,
  std::optional<lsm::proto::manifest> new_manifest) {
    reset_manifest_update update{
      .new_uuid = uuid,
      .new_manifest = std::move(new_manifest),
    };
    auto allowed = update.can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }
    return update;
}

std::expected<std::monostate, lsm_update_error>
reset_manifest_update::can_apply(const lsm_state& state) const {
    if (!state.volatile_buffer.empty()) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Cannot reset manifest: STM already has {} rows",
            state.volatile_buffer.size())});
    }
    // TODO: should allow if there is a manifest that is empty?
    if (state.persisted_manifest.has_value()) {
        return std::unexpected(
          lsm_update_error{fmt::format(
            "Cannot reset manifest: STM already has persisted manifest {}",
            *state.persisted_manifest)});
    }
    return std::monostate{};
}

std::expected<std::monostate, lsm_update_error> reset_manifest_update::apply(
  lsm_state& state, model::term_id t, model::offset o) {
    auto allowed = can_apply(state);
    if (!allowed.has_value()) {
        return std::unexpected(allowed.error());
    }

    state.domain_uuid = new_uuid;

    // Reset the manifest
    int64_t persisted_seqno = new_manifest ? new_manifest->get_last_seqno()
                                           : lsm::sequence_number(0);
    int64_t persisted_epoch = new_manifest ? new_manifest->get_database_epoch()
                                           : 0;
    state.persisted_manifest = std::move(new_manifest);

    // Reset the deltas. These are set based on the persisted values,
    // with the assumption that the STM is being initialized near raft offset 0.
    // The deltas ensure that future raft offsets map correctly to database
    // seqnos/epochs.
    state.seqno_delta = persisted_seqno - o();
    state.db_epoch_delta = persisted_epoch - t();

    // Clear any volatile state since we're resetting
    state.volatile_buffer.clear();

    return std::monostate{};
}

} // namespace cloud_topics::l1
