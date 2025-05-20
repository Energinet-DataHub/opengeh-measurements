ALTER TABLE {bronze_database}.{bronze_submitted_transactions_table}
CLUSTER BY (`timestamp`)
