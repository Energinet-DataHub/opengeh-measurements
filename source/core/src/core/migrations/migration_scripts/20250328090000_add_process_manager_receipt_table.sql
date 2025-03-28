CREATE TABLE IF NOT EXISTS {core_internal}.{process_manager_receipts}
(
    orchestration_instance_id STRING,
    created TIMESTAMP,
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
CLUSTER BY (orchestration_instance_id)
