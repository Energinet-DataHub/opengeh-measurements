ALTER TABLE {gold_database}.{gold_measurements_sap_series}
CLUSTER BY (orchestration_type, metering_point_id, start_time, created)
