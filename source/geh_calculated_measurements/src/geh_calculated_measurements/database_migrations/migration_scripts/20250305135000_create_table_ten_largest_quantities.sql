CREATE TABLE IF NOT EXISTS {catalog_name}.{calculated_measurements_internal_database}.capacity_settlement_ten_largest_quantities
(
    orchestration_instance_id STRING NOT NULL,
    metering_point_id STRING NOT NULL,
    observation_time TIMESTAMP NOT NULL,
    quantity DECIMAL(18, 3) NOT NULL
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
