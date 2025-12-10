load("//bazel:build.bzl", "redpanda_cc_library")

redpanda_cc_library(
    name = "write_batch_row",
    hdrs = [
        "write_batch_row.h",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//src/v/bytes:iobuf",
        "//src/v/serde",
        "@seastar",
    ],
)

redpanda_cc_library(
    name = "state",
    srcs = [
        "state.cc",
    ],
    hdrs = [
        "state.h",
    ],
    implementation_deps = [
        "//src/v/serde:iobuf",
        "//src/v/serde:named_type",
        "//src/v/serde:optional",
        "//src/v/serde:sstring",
        "//src/v/serde:uuid",
        "//src/v/serde:vector",
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":write_batch_row",
        "//src/v/base",
        "//src/v/bytes:iobuf",
        "//src/v/bytes:iobuf_parser",
        "//src/v/cloud_topics/level_one/metastore:domain_uuid",
        "//src/v/container:chunked_vector",
        "//src/v/lsm",
        "//src/v/lsm/proto:manifest_redpanda_proto",
        "//src/v/model",
        "//src/v/serde",
        "@seastar",
    ],
)
