ALTER TABLE {gold_database}.{gold_measurements}
ADD COLUMNS (is_cancelled boolean after transaction_creation_datetime)
GO

ALTER TABLE {silver_database}.{silver_measurements}
DROP COLUMNS (is_deleted)
GO 

UPDATE {gold_database}.{gold_measurements}
SET is_cancelled = false 
WHERE orchestration_type != "migration"
