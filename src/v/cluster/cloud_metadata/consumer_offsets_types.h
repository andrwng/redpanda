/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "cluster/errc.h"
#include "model/fundamental.h"
#include "serde/envelope.h"
#include "utils/fragmented_vector.h"

#include <absl/container/node_hash_map.h>

namespace cluster::cloud_metadata {

// Subset of Kafka protocol structs for committed offsets.

// Committed offsets belonging to a partition.
struct offset_commit_request_partition
  : public serde::envelope<
      offset_commit_request_partition,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::partition_id partition_index{};
    model::offset committed_offset{};
    int32_t committed_leader_epoch{-1};
    int64_t commit_timestamp{-1};
    std::optional<ss::sstring> committed_metadata{};

    auto serde_fields() {
        return std::tie(
          partition_index,
          committed_offset,
          committed_leader_epoch,
          commit_timestamp,
          committed_metadata);
    }

    friend bool operator==(
      const offset_commit_request_partition&,
      const offset_commit_request_partition&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_partition& p) {
        fmt::print(
          o,
          "{{partition_index: {}, committed_offset: {}, "
          "committed_leader_epoch: {}, commit_timestamp: {}, "
          "committed_metadata: {}}}",
          p.partition_index,
          p.committed_offset,
          p.committed_leader_epoch,
          p.commit_timestamp,
          p.committed_metadata);
        return o;
    }
};

// Partition offsets belonging to a topic.
struct offset_commit_request_topic
  : public serde::envelope<
      offset_commit_request_topic,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    model::topic name{};
    fragmented_vector<offset_commit_request_partition> partitions{};

    auto serde_fields() { return std::tie(name, partitions); }

    friend bool operator==(
      const offset_commit_request_topic&, const offset_commit_request_topic&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_topic& t) {
        fmt::print(o, "{{name: {}, partitions: {}}}", t.name, t.partitions);
        return o;
    }
};

// Topic offsets belonging to a group.
struct offset_commit_request_data
  : public serde::envelope<
      offset_commit_request_data,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    ss::sstring group_id{};
    fragmented_vector<offset_commit_request_topic> topics{};

    auto serde_fields() { return std::tie(group_id, topics); }

    friend bool operator==(
      const offset_commit_request_data&, const offset_commit_request_data&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offset_commit_request_data& d) {
        fmt::print(o, "{{group_id: {}, topics: {}}}", d.group_id, d.topics);
        return o;
    }
};

// Request to restore the given groups. It is expected that each group in this
// request maps to the same offset topic partition.
struct offsets_recovery_request
  : public serde::envelope<
      offsets_recovery_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;
    fragmented_vector<offset_commit_request_data> group_data;

    auto serde_fields() { return std::tie(group_data); }

    friend bool
    operator==(const offsets_recovery_request&, const offsets_recovery_request&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_request& r) {
        fmt::print(o, "{{group_data: {}}}", r.group_data);
        return o;
    }
};

// Result of a restore request.
struct offsets_recovery_reply
  : public serde::envelope<
      offsets_recovery_request,
      serde::version<0>,
      serde::compat_version<0>> {
    using rpc_adl_exempt = std::true_type;

    cluster::errc error{};
    fragmented_vector<ss::sstring> failed_group_ids{};

    auto serde_fields() { return std::tie(error, failed_group_ids); }

    friend bool
    operator==(const offsets_recovery_reply&, const offsets_recovery_reply&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& o, const offsets_recovery_reply& r) {
        fmt::print(
          o,
          "{{error: {}, failed_group_ids: {}}}",
          r.error,
          r.failed_group_ids);
        return o;
    }
};

} // namespace cluster::cloud_metadata
