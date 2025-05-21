CREATE TABLE {bronze_database}.{bronze_migrated_transactions_table}
(
    metering_point_id STRING NOT NULL,
    type_of_mp STRING NOT NULL,
    historical_flag STRING NOT NULL,
    resolution STRING NOT NULL,
    transaction_id STRING,
    transaction_insert_date TIMESTAMP NOT NULL,
    unit STRING NOT NULL,
    status INT NOT NULL,
    read_reason STRING NOT NULL,
    valid_from_date TIMESTAMP NOT NULL,
    valid_to_date TIMESTAMP NOT NULL,
    values ARRAY<
        STRUCT<
            position INT,
            quality STRING,
            quantity DECIMAL(18, 6)
        >
    > NOT NULL,
    created_in_migrations TIMESTAMP NOT NULL,
    created TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (metering_point_id, transaction_insert_date, valid_from_date, transaction_id)
TBLPROPERTIES (
    delta.deletedFileRetentionDuration = "interval 30 days",
    delta.logRetentionDuration = "interval 30 days"
)
