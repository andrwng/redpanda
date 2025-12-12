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

#include "cloud_topics/level_one/metastore/lsm/replicated_persistence.h"

#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "model/batch_builder.h"

namespace cloud_topics::l1 {

namespace {

class replicated_metadata_persistence : public lsm::io::metadata_persistence {
public:
    replicated_metadata_persistence(
      stm* stm,
      std::unique_ptr<lsm::io::metadata_persistence> cloud_persistence)
      : _stm(stm)
      , _cloud_persistence(std::move(cloud_persistence)) {}

    ss::future<std::optional<iobuf>>
    read_manifest(lsm::internal::database_epoch epoch) override {
        _as.check();
        auto _ = _gate.hold();
        auto term_result = co_await _stm->sync(std::chrono::seconds(30));
        if (!term_result.has_value()) {
            throw std::runtime_error("Failed to sync when loading manifest");
        }
        auto term = term_result.value();
        uint64_t persisted_epoch = term() + _stm->state().db_epoch_delta;
        if (persisted_epoch > epoch()) {
            throw std::runtime_error(
              fmt::format(
                "Can't load manifest at or below epoch {}, current epoch: {}",
                epoch(),
                persisted_epoch));
        }
        if (!_stm->state().persisted_manifest.has_value()) {
            co_return std::nullopt;
        }
        co_return co_await _stm->state().persisted_manifest->to_proto();
    }

    ss::future<>
    write_manifest(lsm::internal::database_epoch epoch, iobuf b) override {
        auto h = _gate.hold();
        co_await _cloud_persistence->write_manifest(epoch, b.copy());

        // Now that the manifest has be persisted successfully, replicate to
        // the log.

        // XXX store as const in the beginning
        auto domain_uuid = _stm->state().domain_uuid;
        auto m = co_await lsm::proto::manifest::from_proto(std::move(b));
        auto update_res = persist_manifest_update::build(
          _stm->state(), domain_uuid, std::move(m));
        if (!update_res.has_value()) {
            throw std::runtime_error("bad update");
        }
        model::batch_builder builder;
        builder.set_batch_type(model::record_batch_type::l1_stm);
        builder.add_record(
          {.key = serde::to_iobuf(lsm_update_key::persist_manifest),
           .value = co_await serde::to_iobuf_async(
             std::move(update_res.value()))});
        auto batch = co_await std::move(builder).build();

        auto replicate_result = co_await _stm->replicate_and_wait(
          model::term_id(epoch()), std::move(batch), _as);

        if (!replicate_result.has_value()) {
            throw std::runtime_error(
              fmt::format(
                "Replication error after persisting manifest: {}",
                int(replicate_result.error())));
        }
    }

    ss::future<> close() override {
        _as.request_abort();
        auto fut = _gate.close();
        auto persistence_fut = _cloud_persistence->close();
        co_await std::move(persistence_fut);
        co_await std::move(fut);
    }

private:
    ss::gate _gate;
    ss::abort_source _as;
    stm* _stm;
    std::unique_ptr<metadata_persistence> _cloud_persistence;
};

} // namespace

ss::future<std::unique_ptr<lsm::io::metadata_persistence>>
open_replicated_metadata_persistence(
  stm* stm,
  cloud_io::remote* remote,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage_clients::object_key prefix) {
    auto cloud_persistence = co_await lsm::io::open_cloud_metadata_persistence(
      remote, bucket, prefix);
    co_return std::make_unique<replicated_metadata_persistence>(
      stm, std::move(cloud_persistence));
}

} // namespace cloud_topics::l1
