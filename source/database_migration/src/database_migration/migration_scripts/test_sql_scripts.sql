CREATE TABLE test_schema.test_table
(
    created TIMESTAMP NOT NULL,
) USING DELTA
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = true,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
