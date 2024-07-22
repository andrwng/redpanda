find_package(Avro)

set(testdata_dir "${CMAKE_CURRENT_BINARY_DIR}/testdata")
set(testdata_gen "${CMAKE_CURRENT_SOURCE_DIR}/gen_test_iceberg_manifest.py")

set(nested_manifest_avro "${testdata_dir}/nested_manifest.avro")
add_custom_command(
  OUTPUT ${nested_manifest_avro}
  COMMAND ${testdata_gen} -o ${nested_manifest_avro} -n 100
  DEPENDS ${testdata_gen}
  COMMENT "Running test manifest generator"
  VERBATIM)

rp_test(
  UNIT_TEST
  GTEST
  USE_CWD
  BINARY_NAME iceberg
  SOURCES
    datatypes_json_test.cc
    datatypes_test.cc
    manifest_serialization_test.cc
    partition_test.cc
    schema_json_test.cc
    test_schemas.cc
    ${nested_manifest_avro}
  LIBRARIES
    Avro::avro
    Boost::iostreams
    v::bytes
    v::gtest_main
    v::iceberg
    v::utils
  INPUT_FILES
    ${nested_manifest_avro}
  ARGS "-- -c1"
)
