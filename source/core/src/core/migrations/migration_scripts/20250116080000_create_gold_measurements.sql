CREATE TABLE IF NOT EXISTS {gold_database}.{gold_measurements}
(
    metering_point_id STRING,
    observation_time TIMESTAMP,
    quantity DECIMAL(18, 3),
    quality STRING,
    metering_point_type STRING,
    transaction_id STRING,
    transaction_creation_datetime TIMESTAMP,
    created TIMESTAMP,
    modified TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
CLUSTER BY (metering_point_id, observation_time, transaction_creation_datetime)
