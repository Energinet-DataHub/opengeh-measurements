ALTER TABLE {gold_database}.{gold_measurements}
ADD COLUMNS (is_cancelled boolean after transaction_creation_datetime)
GO

UPDATE {gold_database}.{gold_measurements}
SET is_cancelled = false 
GO 

ALTER TABLE {gold_database}.{gold_measurements}
ADD CONSTRAINT is_cancelled_is_not_null_chk CHECK (is_cancelled is not null)
