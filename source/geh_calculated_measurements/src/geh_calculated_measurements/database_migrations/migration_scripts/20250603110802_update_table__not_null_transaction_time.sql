ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
    ADD CONSTRAINT transaction_start_time_not_null_chk
    CHECK (transaction_start_time IS NOT NULL)
GO

ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
    ADD CONSTRAINT transaction_end_time_not_null_chk
    CHECK (transaction_end_time IS NOT NULL)