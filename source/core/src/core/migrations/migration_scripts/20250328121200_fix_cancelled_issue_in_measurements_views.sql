DROP VIEW IF EXISTS {gold_database}.{gold_current_v1}
GO

CREATE VIEW {gold_database}.{gold_current_v1} AS
WITH RankedRows AS (
    SELECT
        metering_point_id,
        observation_time,
        quantity,
        quality,
        metering_point_type,
        is_cancelled,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE
    metering_point_id IS NOT NULL
    AND observation_time IS NOT NULL
    AND quantity IS NOT NULL
    AND quality IS NOT NULL
    AND metering_point_type IS NOT NULL
)
SELECT metering_point_id, observation_time, quantity, quality, metering_point_type
FROM RankedRows
WHERE
    row_num = 1
    AND NOT is_cancelled;

GO

DROP VIEW IF EXISTS {gold_database}.{gold_capacity_settlement_v1}
GO

CREATE VIEW {gold_database}.{gold_capacity_settlement_v1} AS
WITH RankedRows AS (
    SELECT
        metering_point_id,
        quantity,
        observation_time,
        metering_point_type,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE metering_point_id IS NOT NULL
    AND quantity IS NOT NULL
    AND observation_time IS NOT NULL
    AND metering_point_type IS NOT NULL
    AND metering_point_type IN ('consumption', 'capacity_settlement')
)
SELECT metering_point_id, quantity, observation_time, metering_point_type
FROM RankedRows
WHERE
    row_num = 1
    AND NOT is_cancelled;

GO

DROP VIEW IF EXISTS {gold_database}.{gold_electrical_heating_v1}
GO

CREATE VIEW {gold_database}.{gold_electrical_heating_v1} AS
WITH RankedRows AS (
    SELECT
        metering_point_id,
        quantity,
        observation_time,
        metering_point_type,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE metering_point_id IS NOT NULL
    AND quantity IS NOT NULL
    AND observation_time IS NOT NULL
    AND metering_point_type IS NOT NULL
)
SELECT metering_point_id, quantity, observation_time, metering_point_type
FROM RankedRows
WHERE
    row_num = 1
    AND NOT is_cancelled;

