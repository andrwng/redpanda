/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/iobuf.h"
#include "iceberg/values.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/message.h>

namespace datalake {

class struct_converter;

class proto_to_iceberg_converter {
public:
    static proto_to_iceberg_converter
    create(const google::protobuf::Descriptor* desc);

    proto_to_iceberg_converter(
      const google::protobuf::Descriptor* desc,
      std::unique_ptr<struct_converter> converter)
      : pb_desc_(desc)
      , prototype_msg_(factory_.GetPrototype(pb_desc_))
      , struct_converter_(std::move(converter)) {}

    iceberg::value_ptr val_from_pb(iobuf buf) const;

private:
    std::unique_ptr<google::protobuf::Message> msg_from_buf(iobuf buf) const;
    iceberg::value_ptr val_from_msg(const google::protobuf::Message* msg) const;

    google::protobuf::DynamicMessageFactory factory_;
    const google::protobuf::Descriptor* pb_desc_;
    const google::protobuf::Message* prototype_msg_;

    std::unique_ptr<struct_converter> struct_converter_;
};

} // namespace datalake
