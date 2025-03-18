CREATE TABLE {bronze_database}.{submitted_transactions_quarantined_table}
(
    orchestration_type STRING,
    orchestration_instance_id STRING,
    metering_point_id STRING,
    transaction_id STRING,
    transaction_creation_datetime TIMESTAMP,
    metering_point_type STRING,
    unit STRING,
    resolution STRING,
    start_datetime TIMESTAMP,
    end_datetime TIMESTAMP,
    points ARRAY<
        STRUCT<
            position INT,
            quantity DECIMAL(18, 3),
            quality STRING
        >
    >,
    created TIMESTAMP NOT NULL,
    validate_orchestration_type_enum BOOLEAN NOT NULL,
    validate_quality_enum BOOLEAN NOT NULL,
    validate_metering_point_type_enum BOOLEAN NOT NULL,
    validate_unit_enum BOOLEAN NOT NULL,
    validate_resolution_enum BOOLEAN NOT NULL
)
USING DELTA
CLUSTER BY (transaction_id, transaction_creation_datetime, metering_point_id, start_datetime)
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = true,
    delta.autoOptimize.autoCompact = false,
    delta.deletedFileRetentionDuration = 'interval 30 days'
)
