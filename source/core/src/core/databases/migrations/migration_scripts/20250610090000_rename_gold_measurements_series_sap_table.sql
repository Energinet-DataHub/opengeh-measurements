DROP TABLE IF EXISTS {gold_database}.measurements_series_sap;

GO

CREATE TABLE IF NOT EXISTS {gold_database}.{gold_measurements_sap_series}
(
    orchestration_type STRING,
    metering_point_id STRING,
    transaction_id STRING,
    transaction_creation_datetime TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    unit STRING,
    resolution STRING,
    created TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)