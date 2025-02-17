query = """INSERT INTO measurements (
  transaction_id, quantity, transaction_creation_datetime, created, modified, -- dynamic variables
  metering_point_id, observation_time, quality, metering_point_type -- static variables
)
SELECT
    REPLACE(CAST(uuid() AS VARCHAR(50)), '-', '') AS transaction_id, -- transaction_id
    CAST(RAND() * 1000000 AS DECIMAL(18, 3)) AS quantity, -- quantity
    GETDATE() AS transaction_creation_datetime, -- transaction_creation_datetime
    GETDATE() AS created, -- created
    GETDATE() AS modified, -- modified
    '170000030000000201' AS metering_point_id, -- metering_point_id
    '2024-11-30T23:00:00Z' AS observation_time, -- observation_time
    'measured' AS quality, -- quality
    'consumption' AS metering_point_type -- metering_point_type"""
