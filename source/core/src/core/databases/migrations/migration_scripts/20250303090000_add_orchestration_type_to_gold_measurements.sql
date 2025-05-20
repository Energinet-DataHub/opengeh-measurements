ALTER TABLE {gold_database}.{gold_measurements}
    ADD COLUMN orchestration_type STRING AFTER metering_point_id;

GO

UPDATE {gold_database}.{gold_measurements}
SET orchestration_type = 'DUMMY_VALUE'

GO

ALTER TABLE {gold_database}.{gold_measurements}
    ADD CONSTRAINT orchestration_type_not_null CHECK (orchestration_type IS NOT NULL);
