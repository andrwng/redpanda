find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(Protobuf REQUIRED)


v_cc_library(
  NAME datalake
  SRCS
    protobuf_to_arrow_converter.cc
    proto_to_arrow_scalar.cc
    proto_to_arrow_struct.cc
    proto_to_iceberg_converter.cc
    timestamp_partition_constants.cc
  DEPS
    v::iceberg
    Seastar::seastar
    Arrow::arrow_shared
    Parquet::parquet_shared
    protobuf::libprotobuf
)

add_subdirectory(tests)
