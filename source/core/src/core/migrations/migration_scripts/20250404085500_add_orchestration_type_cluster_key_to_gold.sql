ALTER TABLE {gold_database}.{gold_measurements}
CLUSTER BY (metering_point_id, observation_time, transaction_creation_datetime, orchestration_type)
