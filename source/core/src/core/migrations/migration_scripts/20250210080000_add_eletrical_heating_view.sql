CREATE VIEW {gold_database}.electrical_heating_view_v1 AS
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY metering_point_id, observation_time ORDER BY transaction_creation_datetime DESC) AS row_num
    FROM {gold_database}.{gold_measurements}
)
SELECT metering_point_id, quantity, observation_time, metering_point_type
FROM RankedRows
WHERE row_num = 1;
