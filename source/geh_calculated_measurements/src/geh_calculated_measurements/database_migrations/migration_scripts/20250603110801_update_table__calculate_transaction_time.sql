-- Calculate transaction start and end times for each transaction
CREATE TEMPORARY VIEW calculated_measurements_tmp AS
SELECT 
    metering_point_id, 
    transaction_id,
    MIN(observation_time) AS transaction_start_time,
    MAX(observation_time) + INTERVAL '1' HOUR AS transaction_end_time
FROM {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements
GROUP BY metering_point_id, transaction_id
GO

-- Update the calculated_measurements table with the transaction start and end times
MERGE INTO {catalog_name}.{calculated_measurements_internal_database}.calculated_measurements AS calculated_measurements
USING calculated_measurements_tmp AS tmp
ON tmp.transaction_id = calculated_measurements.transaction_id AND tmp.metering_point_id = calculated_measurements.metering_point_id
WHEN MATCHED THEN
    UPDATE SET 
        calculated_measurements.transaction_start_time = tmp.transaction_start_time,
        calculated_measurements.transaction_end_time = tmp.transaction_end_time;
GO

-- Clean up the temporary view
DROP VIEW calculated_measurements_tmp;