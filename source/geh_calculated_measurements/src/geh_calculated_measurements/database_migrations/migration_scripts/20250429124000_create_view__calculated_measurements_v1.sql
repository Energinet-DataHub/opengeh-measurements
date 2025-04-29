DROP VIEW IF EXISTS {catalog_name}.{calculated_measurements_database}.hourly_calculated_measurements_v1
GO

CREATE VIEW {catalog_name}.{calculated_measurements_database}.calculated_measurements_v1 AS
SELECT 
  orchestration_type,
  orchestration_instance_id,
  transaction_id,
  transaction_creation_datetime,
  metering_point_id,
  metering_point_type,
  observation_time,
  quantity,
  "kWh" as quantity_unit,
  "calculated" as quantity_quality,
  "PT1H" as resolution
FROM {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
WHERE observation_time >= current_timestamp() - INTERVAL '3' YEAR
GO
