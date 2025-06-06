ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
ADD COLUMNS (transaction_start_time TIMESTAMP, transaction_end_time TIMESTAMP)
