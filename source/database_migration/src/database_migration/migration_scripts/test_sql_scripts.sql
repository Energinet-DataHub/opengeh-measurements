CREATE TABLE bronze_schema.bronze_time_series_table
(
    metering_point STRUCT<
        masterdata ARRAY<
            STRUCT<
                grid_area STRING,
                masterdata_end_date TIMESTAMP,
                masterdata_start_date TIMESTAMP,
                type_of_mp STRING
            >
        >,
        metering_point_id STRING
    >,
    time_series ARRAY<
        STRUCT<
            historical_flag STRING,
            resolution STRING,
            transaction_id STRING,
            message_id STRING,
            transaction_insert_date TIMESTAMP,
            unit STRING,
            status INT,
            read_reason STRING,
            valid_from_date TIMESTAMP,
            valid_to_date TIMESTAMP,
            values ARRAY<
                STRUCT<
                    position INT,
                    quality STRING,
                    quantity DECIMAL(18, 6)
                >
            >
        >
    >,
    _rescued_data STRING,
    created TIMESTAMP NOT NULL,
    file_path STRING NOT NULL
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
