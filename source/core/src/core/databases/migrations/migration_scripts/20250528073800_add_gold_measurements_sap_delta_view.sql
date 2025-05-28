CREATE VIEW {gold_database}.{gold_sap_delta_v1} AS
SELECT
    serie_no, metering_point_id, transaction_id, transaction_creation_datetime,
    start_time, end_time, unit, resolution, is_cancelled, created
FROM {gold_database}.{gold_measurements}
