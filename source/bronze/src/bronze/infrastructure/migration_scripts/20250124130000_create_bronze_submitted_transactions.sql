CREATE TABLE {bronze_database}.{bronze_submitted_transactions}
(
    key BINARY,
    value BINARY,
    topic STRING,
    partition INT,
    offset LONG,
    timestamp TIMESTAMP,
    timestamp_type INT,
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
