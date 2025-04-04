CREATE TABLE IF NOT EXISTS {catalog_name}.measurements_calculated_internal.missing_measurements_log
(
    orchestration_instance_id STRING NOT NULL,
    metering_point_id STRING NOT NULL,
    date TIMESTAMP NOT NULL    
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days',
    delta.columnMapping.mode = 'name',
    delta.minReaderVersion = '2',
    delta.minWriterVersion = '5'
)
CLUSTER BY (orchestration_instance_id, metering_point_id)