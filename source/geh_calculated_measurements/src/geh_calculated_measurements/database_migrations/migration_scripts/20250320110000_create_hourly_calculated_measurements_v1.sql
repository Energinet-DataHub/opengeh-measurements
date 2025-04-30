DROP VIEW IF EXISTS {catalog_name}.{calculated_measurements_database}.hourly_calculated_measurements_v1
GO

CREATE OR REPLACE VIEW {catalog_name}.{calculated_measurements_database}.hourly_calculated_measurements_v1 AS
WITH _input AS (
  SELECT 
    orchestration_type,
    orchestration_instance_id,
    transaction_id,
    transaction_creation_datetime,
    metering_point_id,
    metering_point_type,
    date,
    quantity
  FROM {catalog_name}.measurements_calculated_internal.calculated_measurements 
),
_hours AS (
  SELECT 
    _input.orchestration_type,
    _input.orchestration_instance_id,
    _input.transaction_id,
    _input.transaction_creation_datetime,
    _input.metering_point_id,
    _input.metering_point_type,
    _input.date,
    _input.quantity,
    explode(sequence(
      to_utc_timestamp(FROM_UTC_TIMESTAMP(_input.date, 'Europe/Copenhagen'), 'Europe/Copenhagen'),
      to_utc_timestamp(date_add(FROM_UTC_TIMESTAMP(_input.date, 'Europe/Copenhagen'), 1) - interval 1 hour, 'Europe/Copenhagen'),
      interval 1 hour
    )) as hour
  FROM _input
)
SELECT 
  _hours.orchestration_type,
  _hours.orchestration_instance_id,
  _hours.transaction_id,
  _hours.transaction_creation_datetime,
  _hours.metering_point_id,
  _hours.metering_point_type,
  _hours.hour as observation_time,
  CASE WHEN _hours.hour = to_utc_timestamp(FROM_UTC_TIMESTAMP(_hours.date, 'Europe/Copenhagen'), 'Europe/Copenhagen')
    THEN _hours.quantity
    ELSE 0
  END AS quantity,
  "calculated" as quantity_quality,
  "kWh" as quantity_unit,
  "PT1H" as resolution
FROM _hours
