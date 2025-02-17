CREATE TABLE IF NOT EXISTS {bronze_database}.measurements
(
    orchestration_type STRING,
    orchestration_instance_id STRING,
    metering_point_id STRING,
    transaction_id STRING,
    transaction_creation_datetime TIMESTAMP,
    metering_point_type STRING,
    product STRING,
    unit STRING,
    resolution STRING,
    start_datetime TIMESTAMP,
    end_datetime TIMESTAMP,
    points ARRAY<
        STRUCT<
            position INT,
            quantity DECIMAL(18, 3),
            quality STRING
        >
    >,
    _rescued_data STRING,
    created TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
