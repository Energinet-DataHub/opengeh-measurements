ALTER TABLE {silver_database}.{silver_measurements_table}
ADD COLUMNS (is_cancelled boolean after points, is_deleted boolean after is_cancelled)
GO 

UPDATE TABLE {silver_database}.{silver_measurements_table}
SET is_cancelled = False, is_deleted = False
GO 

ALTER TALBE {silver_database}.{silver_measurements_table}
ADD CONSTRAINT is_cancelled_is_not_null_chk CHECK is_cancelled is not null
GO

ALTER TALBE {silver_database}.{silver_measurements_table}
ADD CONSTRAINT is_deleted_is_not_null_chk CHECK is_deleted is not null
