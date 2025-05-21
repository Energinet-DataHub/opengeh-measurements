DROP VIEW IF EXISTS {gold_database}.{gold_current_v1}
GO

CREATE VIEW {gold_database}.{gold_current_v1} AS
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
    WHERE
    metering_point_id IS NOT NULL
    AND observation_time IS NOT NULL
    AND quality IS NOT NULL
    AND metering_point_type IS NOT NULL
    AND NOT is_cancelled
)
SELECT metering_point_id, observation_time, quantity, quality, metering_point_type
FROM RankedRows
WHERE row_num = 1
