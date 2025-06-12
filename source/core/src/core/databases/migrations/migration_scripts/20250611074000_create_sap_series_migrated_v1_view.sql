CREATE VIEW {gold_database}.{gold_sap_series_migrated_v1} AS
SELECT
    metering_point_id, transaction_id, transaction_creation_datetime,
    start_time, end_time, unit, resolution, created
FROM {gold_database}.{gold_measurements_sap_series}
WHERE orchestration_type = 'migration'

