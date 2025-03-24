ALTER TABLE {silver_database}.{silver_measurements_table}
DROP CONSTRAINT IF EXISTS is_deleted_is_not_null_chk
GO

ALTER TABLE {silver_database}.{silver_measurements_table}
DROP COLUMNS IF EXISTS (is_deleted)
GO

ALTER TABLE {bronze_database}.{submitted_transactions_quarantined_table}
DROP COLUMNS IF EXISTS (is_deleted)