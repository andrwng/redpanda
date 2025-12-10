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

#include "bytes/iobuf.h"
#include "serde/envelope.h"

#include <seastar/core/sstring.hh>

namespace cloud_topics::l1 {

struct write_batch_row
  : public serde::
      envelope<write_batch_row, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const write_batch_row&, const write_batch_row&)
      = default;
    auto serde_fields() { return std::tie(key, value); }

    // NOTE: user-space key.
    ss::sstring key;

    iobuf value;
};

} // namespace cloud_topics::l1
