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

#include "metrics/metrics.h"
#include "model/fundamental.h"

namespace datalake {

class translation_probe final {
public:
    // Note: Do not forget to register new causes in
    // register_invalid_record_metric.
    enum class invalid_record_cause {
        /// Failed to resolve the Kafka schema for the record. This covers the
        /// cases where the magic byte is missing from the record or schema id
        /// refers to a non-existent schema.
        failed_kafka_schema_resolution,
        /// Failed to translate the record data according to the schema fetched
        /// from the schema registry to an equivalent Iceberg schema/Parquet
        /// format.
        failed_data_translation,
        /// Failed to ensure the table schema matches the inferred Iceberg
        /// schema.
        failed_iceberg_schema_resolution,
    };

public:
    explicit translation_probe(model::ntp ntp);

public:
    void increment_invalid_record(invalid_record_cause cause) {
        counter_ref(cause)++;
    }

    void increment_raw_bytes_processed(size_t b) { raw_bytes_processed += b; }
    void increment_raw_bytes_translated(size_t b) { raw_bytes_translated += b; }
    void increment_decompressed_bytes_translated(size_t b) {
        decompressed_bytes_translated += b;
    }

    size_t& counter_ref(invalid_record_cause cause) {
        switch (cause) {
        case invalid_record_cause::failed_kafka_schema_resolution:
            return _num_failed_kafka_schema_resolution;
        case invalid_record_cause::failed_data_translation:
            return _num_failed_data_translation;
        case invalid_record_cause::failed_iceberg_schema_resolution:
            return _num_failed_iceberg_schema_resolution;
        }
    }

private:
    void register_invalid_record_metric();
    void register_throughput_metrics();

private:
    model::ntp _ntp;
    std::optional<metrics::public_metric_groups> _public_metrics;

    size_t _num_failed_kafka_schema_resolution = 0;
    size_t _num_failed_data_translation = 0;
    size_t _num_failed_iceberg_schema_resolution = 0;

    // Raw bytes consumed for translation that may or may not succeed in being
    // translated. For example, if we fail to communicate with the coordinator,
    // preventing translation of a batch, this metric still ticks up.
    size_t raw_bytes_processed = 0;

    // Raw bytes consumed for translation that were successfully translated.
    // Note that translation into the DLQ is still considered a success.
    size_t raw_bytes_translated = 0;

    // Bytes post-decompression consumed for translation that were successfully
    // translated. Note that translation into the DLQ is still considered a
    // success.
    size_t decompressed_bytes_translated = 0;
};

std::ostream&
operator<<(std::ostream& os, translation_probe::invalid_record_cause cause);

}; // namespace datalake
