/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/probe.h"

#include <fmt/format.h>

namespace datalake {

std::ostream& operator<<(std::ostream& os, const runner_metrics& m) {
    os << fmt::format(
      "translators_idle: {}, translators_reconciling: {}, "
      "translators_awaiting_data: {}, "
      "translators_awaiting_scheduling: {}, translators_translating: {}, "
      "translators_finishing: {}, translators_pending_finish: {}",
      m.translators_idle,
      m.translators_reconciling,
      m.translators_awaiting_data,
      m.translators_awaiting_scheduling,
      m.translators_translating,
      m.translators_finishing,
      m.translators_pending_finish);
    return os;
}

} // namespace datalake
