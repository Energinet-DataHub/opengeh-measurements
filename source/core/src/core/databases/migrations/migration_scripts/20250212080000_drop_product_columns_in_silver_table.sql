ALTER TABLE {silver_database}.{silver_measurements_table}
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')

GO

ALTER TABLE {silver_database}.{silver_measurements_table}
DROP COLUMN product