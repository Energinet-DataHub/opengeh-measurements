ALTER TABLE {gold_database}.{gold_measurements}
ADD COLUMNS (is_cancelled boolean after transaction_creation_datetime)
GO

UPDATE {gold_database}.{gold_measurements}
SET is_cancelled = false 
WHERE orchestration_type != "migration"
