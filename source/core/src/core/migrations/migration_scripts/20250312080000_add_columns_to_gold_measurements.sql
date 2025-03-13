ALTER TABLE {gold_database}.{gold_measurements}
ADD COLUMNS (unit string after metering_point_type, orchestration_instance_id string after orchestration_type, resolution string after unit)
GO

UPDATE {gold_database}.{gold_measurements}
SET
  unit = "UNKNOWN",
  orchestration_instance_id = "UNKNOWN",
  resolution = "UNKNOWN"
GO

ALTER TABLE {gold_database}.{gold_measurements}
ADD CONSTRAINT unit_is_not_null_chk CHECK (unit is not null)
GO

ALTER TABLE {gold_database}.{gold_measurements}
ADD CONSTRAINT orchestration_instance_id_is_not_null_chk CHECK (orchestration_instance_id is not null)
GO

ALTER TABLE {gold_database}.{gold_measurements}
ADD CONSTRAINT resolution_is_not_null_chk CHECK (resolution is not null)
