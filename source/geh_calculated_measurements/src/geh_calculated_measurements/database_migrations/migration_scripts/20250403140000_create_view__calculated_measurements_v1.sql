DROP VIEW IF EXISTS {catalog_name}.measurements_calculated.hourly_calculated_measurements_v1
GO

CREATE VIEW {catalog_name}.measurements_calculated.calculated_measurements_v1 AS
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
FROM {catalog_name}.measurements_calculated_internal.calculated_measurements 
