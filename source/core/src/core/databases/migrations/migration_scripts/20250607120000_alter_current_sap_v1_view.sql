CREATE VIEW {gold_database}.{gold_current_sap_v1} AS
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
)
SELECT metering_point_id, transaction_id, transaction_creation_datetime, observation_time, quantity, quality, created
FROM RankedRows
WHERE 
    row_num = 1
    AND NOT is_cancelled;

