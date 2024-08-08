/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/proto_to_iceberg_converter.h"

#include "base/vlog.h"
#include "bytes/streambuf.h"
#include "datalake/logger.h"
#include "iceberg/values.h"

namespace pb = google::protobuf;

namespace datalake {

namespace {

// The converters expect that the message is valid protobuf, and that the field
// descriptors correspond to the messages correctly. This is because we parse
// the message with the prototype.

class converter_interface {
public:
    virtual iceberg::value_ptr make_value(
      const google::protobuf::Message* parent_msg,
      const google::protobuf::FieldDescriptor* field_desc) const
      = 0;
    virtual ~converter_interface() = default;
};

class bool_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(iceberg::boolean_value{
          parent_msg->GetReflection()->GetBool(*parent_msg, field_desc)});
    }
};

class int_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(iceberg::int_value{
          parent_msg->GetReflection()->GetInt32(*parent_msg, field_desc)});
    }
};

class long_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(iceberg::long_value{
          parent_msg->GetReflection()->GetInt64(*parent_msg, field_desc)});
    }
};

class float_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(iceberg::float_value{
          parent_msg->GetReflection()->GetFloat(*parent_msg, field_desc)});
    }
};

class double_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(iceberg::double_value{
          parent_msg->GetReflection()->GetDouble(*parent_msg, field_desc)});
    }
};

class string_converter : public converter_interface {
public:
    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        return std::make_unique<iceberg::value>(
          iceberg::string_value{iobuf::from(
            parent_msg->GetReflection()->GetString(*parent_msg, field_desc))});
    }
};
} // namespace

class struct_converter : public converter_interface {
public:
    using converters_list_t
      = chunked_vector<std::unique_ptr<converter_interface>>;
    static std::unique_ptr<struct_converter>
    create(const pb::Descriptor* msg_desc) {
        converters_list_t child_converters;
        child_converters.reserve(msg_desc->field_count());
        for (int field_idx = 0; field_idx < msg_desc->field_count();
             ++field_idx) {
            const auto* child_desc = msg_desc->field(field_idx);
            switch (child_desc->type()) {
            case pb::FieldDescriptor::TYPE_INT32:
                child_converters.emplace_back(
                  std::make_unique<int_converter>());
                break;
            case pb::FieldDescriptor::TYPE_INT64:
                child_converters.emplace_back(
                  std::make_unique<long_converter>());
                break;
            case pb::FieldDescriptor::TYPE_BOOL:
                child_converters.emplace_back(
                  std::make_unique<bool_converter>());
                break;
            case pb::FieldDescriptor::TYPE_FLOAT:
                child_converters.emplace_back(
                  std::make_unique<float_converter>());
                break;
            case pb::FieldDescriptor::TYPE_DOUBLE:
                child_converters.emplace_back(
                  std::make_unique<double_converter>());
                break;
            case pb::FieldDescriptor::TYPE_STRING:
                child_converters.emplace_back(
                  std::make_unique<string_converter>());
                break;
            case pb::FieldDescriptor::TYPE_MESSAGE:
                // TODO: handle lists and maps.
                child_converters.emplace_back(
                  struct_converter::create(child_desc->message_type()));
                break;
            case pb::FieldDescriptor::TYPE_BYTES:
            case pb::FieldDescriptor::TYPE_GROUP:
            case pb::FieldDescriptor::TYPE_UINT32:
            case pb::FieldDescriptor::TYPE_UINT64:
            case pb::FieldDescriptor::TYPE_ENUM:
            case pb::FieldDescriptor::TYPE_FIXED32:
            case pb::FieldDescriptor::TYPE_FIXED64:
            case pb::FieldDescriptor::TYPE_SFIXED32:
            case pb::FieldDescriptor::TYPE_SFIXED64:
            case pb::FieldDescriptor::TYPE_SINT32:
            case pb::FieldDescriptor::TYPE_SINT64:
                throw std::invalid_argument(
                  fmt::format("Unsupported type", child_desc->cpp_type_name()));
            }
        }
        return std::make_unique<struct_converter>(std::move(child_converters));
    }
    explicit struct_converter(converters_list_t children)
      : child_converters_(std::move(children)) {}

    iceberg::value_ptr make_struct_value(const pb::Message* struct_msg) const {
        const auto* struct_desc = struct_msg->GetDescriptor();
        if (
          static_cast<size_t>(struct_desc->field_count())
          != child_converters_.size()) {
            // Expected because we constructed converters based on the same
            // message descriptor used to parse the message.
            throw std::invalid_argument(fmt::format(
              "Expected {} children, got {}",
              child_converters_.size(),
              struct_desc->field_count()));
        }
        iceberg::struct_value ret;
        for (int field_idx = 0; field_idx < struct_desc->field_count();
             ++field_idx) {
            const auto& child_converter = *child_converters_[field_idx];
            child_converter.make_value(
              struct_msg, struct_desc->field(field_idx));
        }
        return std::make_unique<iceberg::value>(std::move(ret));
    }

    iceberg::value_ptr make_value(
      const pb::Message* parent_msg,
      const pb::FieldDescriptor* field_desc) const override {
        auto* struct_msg = &parent_msg->GetReflection()->GetMessage(
          *parent_msg, field_desc);
        return make_struct_value(struct_msg);
    }

private:
    converters_list_t child_converters_;
};

proto_to_iceberg_converter create(const google::protobuf::Descriptor* desc) {
    auto struct_converter = struct_converter::create(desc);
    return {desc, std::move(struct_converter)};
}

std::unique_ptr<pb::Message>
proto_to_iceberg_converter::msg_from_buf(iobuf buf) const {
    auto mutable_msg = std::unique_ptr<pb::Message>(prototype_msg_->New());
    iobuf_istream is{std::move(buf)};
    if (!mutable_msg->ParseFromIstream(&is.istream())) {
        vlog(
          datalake_log.debug,
          "Failed to parse protobuf message. Protobuf error string: \"{}\"",
          mutable_msg->InitializationErrorString());
        return nullptr;
    }
    return mutable_msg;
}

iceberg::value_ptr proto_to_iceberg_converter::val_from_pb(iobuf buf) const {
    auto msg = msg_from_buf(std::move(buf));
    if (!msg) {
        throw std::invalid_argument("Failed to parse protobuf");
    }
    return val_from_msg(msg.get());
}

iceberg::value_ptr
proto_to_iceberg_converter::val_from_msg(const pb::Message* msg) const {
    return struct_converter_->make_struct_value(msg);
}

} // namespace datalake
