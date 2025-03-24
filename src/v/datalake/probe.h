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

#include <optional>
#include <ostream>

namespace datalake {

struct runner_metrics {
    struct scoped_count_tick {
        explicit scoped_count_tick(size_t* count)
          : count(count) {
            ++*count;
        }
        ~scoped_count_tick() {
            if (count) {
                --*count;
            }
        }
        scoped_count_tick(const scoped_count_tick&) = delete;

        size_t* count;
    };
    std::optional<scoped_count_tick> scoped_increment_translators_idle() {
        return std::make_optional<scoped_count_tick>(&translators_idle);
    }
    std::optional<scoped_count_tick>
    scoped_increment_translators_reconciling() {
        return std::make_optional<scoped_count_tick>(&translators_reconciling);
    }
    std::optional<scoped_count_tick>
    scoped_increment_translators_awaiting_data() {
        return std::make_optional<scoped_count_tick>(
          &translators_awaiting_data);
    }
    std::optional<scoped_count_tick>
    scoped_increment_translators_awaiting_scheduling() {
        return std::make_optional<scoped_count_tick>(
          &translators_awaiting_scheduling);
    }
    std::optional<scoped_count_tick>
    scoped_increment_translators_translating() {
        return std::make_optional<scoped_count_tick>(&translators_translating);
    }
    std::optional<scoped_count_tick> scoped_increment_translators_finishing() {
        return std::make_optional<scoped_count_tick>(&translators_finishing);
    }
    std::unique_ptr<scoped_count_tick>
    scoped_increment_translators_pending_finish() {
        return std::make_unique<scoped_count_tick>(&translators_pending_finish);
    }

    size_t translators_idle = 0;
    size_t translators_reconciling = 0;
    size_t translators_awaiting_data = 0;
    size_t translators_awaiting_scheduling = 0;
    size_t translators_translating = 0;
    size_t translators_finishing = 0;

    size_t translators_pending_finish = 0;
};
std::ostream& operator<<(std::ostream&, const runner_metrics&);

} // namespace datalake
