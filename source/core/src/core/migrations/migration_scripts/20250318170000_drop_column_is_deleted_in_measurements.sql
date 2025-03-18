ALTER TABLE {silver_database}.{silver_measurements_table}
DROP COLUMNS (is_deleted) 
GO 

ALTER TABLE {bronze_database}.{submitted_transactions_quarantined_table}
DROP COLUMNS (is_deleted)

