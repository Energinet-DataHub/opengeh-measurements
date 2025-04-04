CREATE VIEW {catalog_name}.measurements_calculated.missing_measurements_log_v1 AS
SELECT orchestration_instance_id,
       metering_point_id,
       date        
FROM {catalog_name}.measurements_calculated_internal.missing_measurements_log