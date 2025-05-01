-- Rename column date to observation_time
-- Renaming a column in a Delta table requires proper configuration of the table properties.

ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
SET TBLPROPERTIES (
                  'delta.columnMapping.mode' = 'name',
                  'delta.minReaderVersion' = '2',
                  'delta.minWriterVersion' = '5')
GO

ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
GO

ALTER TABLE {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
RENAME COLUMN date TO observation_time
