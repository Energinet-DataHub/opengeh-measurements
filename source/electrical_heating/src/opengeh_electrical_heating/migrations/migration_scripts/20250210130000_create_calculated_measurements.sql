CREATE TABLE IF NOT EXISTS {calculated_measurements_database}.{calculated_measurements_table}
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
    observation_time TIMESTAMP,
    quantity DECIMAL(18, 3)
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
