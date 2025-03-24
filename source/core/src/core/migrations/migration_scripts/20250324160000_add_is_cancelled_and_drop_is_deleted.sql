-- Add is_cancelled to gold measurements.
ALTER TABLE {gold_database}.{gold_measurements}
ADD COLUMNS (is_cancelled boolean after transaction_creation_datetime)
GO

UPDATE {gold_database}.{gold_measurements}
SET is_cancelled = false 
GO 

ALTER TABLE {gold_database}.{gold_measurements}
ADD CONSTRAINT is_cancelled_is_not_null_chk CHECK (is_cancelled is not null)
GO 

-- Drop is_deleted from silver and bronze measurements.
ALTER TABLE {silver_database}.{silver_measurements_table}
DROP CONSTRAINT IF EXISTS is_deleted_is_not_null_chk
GO

ALTER TABLE {silver_database}.{silver_measurements_table}
DROP COLUMNS IF EXISTS (is_deleted)
GO

ALTER TABLE {bronze_database}.{submitted_transactions_quarantined_table}
DROP COLUMNS IF EXISTS (is_deleted)
GO

-- Use gold is_cancelled in capacity_settlement_view.
DROP VIEW IF EXISTS {gold_database}.{gold_capacity_settlement_v1}
GO

CREATE VIEW {gold_database}.{gold_capacity_settlement_v1} AS
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE metering_point_id IS NOT NULL
    AND quantity IS NOT NULL
    AND observation_time IS NOT NULL
    AND metering_point_type IS NOT NULL
    AND metering_point_type IN ('consumption', 'capacity_settlement')
    AND NOT is_cancelled
)
SELECT metering_point_id, quantity, observation_time, metering_point_type
FROM RankedRows
WHERE row_num = 1;
GO 

-- Use gold is_cancelled in electrical_heating_view.
DROP VIEW IF EXISTS {gold_database}.{gold_electrical_heating_v1}
GO

CREATE VIEW {gold_database}.{gold_electrical_heating_v1} AS
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE metering_point_id IS NOT NULL
    AND quantity IS NOT NULL
    AND observation_time IS NOT NULL
    AND metering_point_type IS NOT NULL
    AND NOT is_cancelled
)
SELECT metering_point_id, quantity, observation_time, metering_point_type
FROM RankedRows
WHERE row_num = 1;

