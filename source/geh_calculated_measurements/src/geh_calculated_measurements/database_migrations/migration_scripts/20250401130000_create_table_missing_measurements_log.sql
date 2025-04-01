CREATE TABLE IF NOT EXISTS {catalog_name}.measurements_calculated_internal.missing_measurements_log
(
    orchestration_instance_id STRING NOT NULL,
    calculation_year INT NOT NULL,
    calculation_month INT NOT NULL,
    execution_time TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
