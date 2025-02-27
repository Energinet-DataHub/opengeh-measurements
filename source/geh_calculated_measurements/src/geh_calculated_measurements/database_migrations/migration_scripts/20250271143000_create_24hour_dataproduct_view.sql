-- DROP VIEW IF EXISTS test_view
-- GO

-- DROP TABLE IF EXISTS {catalog_name}.{calculated_measurements_internal_database}.{calculated_measurements_internal_table}
-- GO

-- CREATE TABLE IF NOT EXISTS {catalog_name}.{calculated_measurements_internal_database}.{calculated_measurements_internal_table}
--  (
--     orchestration_type STRING NOT NULL,
--     orchestration_instance_id STRING NOT NULL,
--     metering_point_id STRING NOT NULL,
--     transaction_id STRING NOT NULL,
--     transaction_creation_datetime TIMESTAMP NOT NULL,
--     metering_point_type STRING NOT NULL,
--     date TIMESTAMP NOT NULL,
--     quantity DECIMAL(18, 3) NOT NULL
-- )
-- GO

-- INSERT INTO {catalog_name}.{calculated_measurements_internal_database}.{calculated_measurements_internal_table} VALUES
-- ('test_type','test_id','test_mp_id','test_id',to_timestamp('2025-03-29 23:00:00'),'test_mp_type',to_timestamp('2025-03-29 23:00:00'),1.1)
-- -- ('test_type','test_id','test_mp_id','test_trans_id',datetime(2024,3,30),'test_mp_type',datetime(2024,3,30),'1.1'),
-- -- ('test_type','test_id','test_mp_id','test_trans_id',datetime(2025,3,30),'test_mp_type',datetime(2024,3,30),'1.1')
-- GO

CREATE OR REPLACE VIEW test_view AS
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
  FROM {catalog_name}.{calculated_measurements_internal_database}.{calculated_measurements_internal_table} 
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
      to_utc_timestamp(date_add(FROM_UTC_TIMESTAMP(_input.date, 'Europe/Copenhagen'), 1), 'Europe/Copenhagen'),
      interval 1 hour
    )) as hour
  FROM _input
)
SELECT 
  _hours.orchestration_type,
  _hours.orchestration_instance_id,
  _hours.transaction_id, --TODO: Change this to actually have the correct format
  _hours.transaction_creation_datetime,
  _hours.metering_point_id,
  _hours.metering_point_type,
  _hours.hour as date,
  CASE WHEN _hours.hour = to_utc_timestamp(FROM_UTC_TIMESTAMP(_hours.date, 'Europe/Copenhagen'), 'Europe/Copenhagen')
    THEN _hours.quantity
    ELSE 0
  END AS quantity
FROM _hours
