DROP TABLE IF EXISTS {gold_database}.{gold_measurements_series_sap};

GO

CREATE TABLE IF NOT EXISTS {gold_database}.{gold_measurements_series_sap}
(
    dh3_serie_seq_no GENERATED ALWAYS AS IDENTITY (START 200000000000 INCREMENT 1),
    dh2_serie_seq_no DECIMAL(14, 0),
    orchestration_type STRING,
    metering_point_id STRING,
    transaction_id STRING,
    transaction_creation_datetime TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    unit STRING,
    resolution STRING,
    is_cancelled BOOLEAN,
    created TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
