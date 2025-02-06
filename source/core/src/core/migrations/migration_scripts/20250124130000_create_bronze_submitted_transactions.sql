CREATE TABLE IF NOT EXISTS {bronze_database}.{bronze_submitted_transactions_table}
(
    key BINARY,
    value BINARY,
    topic STRING,
    partition INT,
    offset LONG,
    timestamp TIMESTAMP,
    timestampType INT
)
USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
