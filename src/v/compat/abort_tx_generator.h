/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/types.h"
#include "compat/generator.h"
#include "compat/tx_gateway_generator.h"
#include "model/tests/randoms.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<cluster::abort_tx_request> {
    static cluster::abort_tx_request random() {
        return cluster::abort_tx_request(
          model::random_ntp(),
          model::random_producer_identity(),
          tests::random_named_int<model::tx_seq>(),
          tests::random_duration<model::timeout_clock::duration>());
    }
    static std::vector<cluster::abort_tx_request> limits() {
        return {
          cluster::abort_tx_request(
            model::random_ntp(),
            model::random_producer_identity(),
            model::tx_seq(std::numeric_limits<int64_t>::min()),
            std::chrono::milliseconds::min()),
          cluster::abort_tx_request(
            model::random_ntp(),
            model::random_producer_identity(),
            model::tx_seq(std::numeric_limits<int64_t>::max()),
            std::chrono::milliseconds::max()),
        };
    }
};

template<>
struct instance_generator<cluster::abort_tx_reply> {
    static cluster::abort_tx_reply random() {
        return cluster::abort_tx_reply(
          instance_generator<cluster::tx_errc>::random());
    }
    static std::vector<cluster::abort_tx_reply> limits() {
        return {
          cluster::abort_tx_reply(cluster::tx_errc::none),
          cluster::abort_tx_reply(cluster::tx_errc::invalid_txn_state),
        };
    }
};

} // namespace compat
