ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.capacity_settlement_calculations
CLUSTER BY (orchestration_instance_id, execution_time)

