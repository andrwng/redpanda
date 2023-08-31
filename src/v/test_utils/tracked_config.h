/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/base_property.h"
#include "config/configuration.h"

// RAII wrapper around config::shard_local_cfg() that tracks get() calls and
// resets any properties that were potentially mutated upon destructing.
class tracked_local_config {
public:
    ~tracked_local_config() {
        for (auto& c : _configs_to_reset) {
            config::shard_local_cfg().get(c).reset();
        }
    }

    config::base_property& get(const std::string_view& name) {
        _configs_to_reset.emplace_back(name);
        return config::shard_local_cfg().get(name);
    }

private:
    std::list<std::string_view> _configs_to_reset;
};
