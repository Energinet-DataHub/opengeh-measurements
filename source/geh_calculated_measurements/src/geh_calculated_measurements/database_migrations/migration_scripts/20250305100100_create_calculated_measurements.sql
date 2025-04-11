CREATE TABLE IF NOT EXISTS {catalog_name}.measurements_calculated_internal.calculated_measurements
(
    orchestration_type STRING NOT NULL,
    orchestration_instance_id STRING NOT NULL,
    metering_point_id STRING NOT NULL,
    transaction_id STRING NOT NULL,
    transaction_creation_datetime TIMESTAMP NOT NULL,
    metering_point_type STRING NOT NULL,
    date TIMESTAMP NOT NULL,
    quantity DECIMAL(18, 3) NOT NULL
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
