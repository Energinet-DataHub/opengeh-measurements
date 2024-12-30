CREATE TABLE {bronze_database}.{bronze_measurements_table}
(
    orchestration_type STRING NOT NULL,
    orchestration_instance_id STRING NOT NULL,
    metering_point_id STRING NOT NULL,
    transaction_id STRING NOT NULL,
    transaction_creation_datetime TIMESTAMP NOT NULL,
    metering_point_type STRING NOT NULL,
    product STRING NOT NULL,
    unit STRING NOT NULL,
    resolution STRING NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    points ARRAY<
        STRUCT<
            position INT,
            quantity DECIMAL(18, 6),
            quality STRING
        >
    > NOT NULL,
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
