load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "replicated_db_test",
    timeout = "moderate",
    srcs = [
        "replicated_db_test.cc",
    ],
    cpu = 1,
    deps = [
        "//src/v/cloud_io:remote",
        "//src/v/cloud_io/tests:s3_imposter",
        "//src/v/cloud_io/tests:scoped_remote",
        "//src/v/cloud_storage_clients",
        "//src/v/cloud_topics/level_one/metastore/lsm:replicated_db",
        "//src/v/cloud_topics/level_one/metastore/lsm:stm",
        "//src/v/cloud_topics/level_one/metastore/lsm:write_batch_row",
        "//src/v/config",
        "//src/v/container:chunked_vector",
        "//src/v/lsm/io:persistence",
        "//src/v/lsm/io:cloud_persistence",
        "//src/v/test_utils:tmp_dir",
        "//src/v/model",
        "//src/v/raft/tests:raft_fixture",
        "//src/v/random:generators",
        "//src/v/test_utils:gtest",
        "//src/v/test_utils:scoped_config",
        "@googletest//:gtest",
        "@seastar",
    ],
)
