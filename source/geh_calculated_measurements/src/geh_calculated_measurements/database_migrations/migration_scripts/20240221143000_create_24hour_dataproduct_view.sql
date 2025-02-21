%sql
CREATE VIEW test_view AS
WITH _input AS (
  SELECT * FROM ctl_shres_d_we_002.test.xhtca
),
hours AS (
  SELECT explode(sequence(
    to_utc_timestamp(FROM_UTC_TIMESTAMP(_input.date, 'Europe/Copenhagen'), 'Europe/Copenhagen'),
    to_utc_timestamp(date_add(FROM_UTC_TIMESTAMP(_input.date, 'Europe/Copenhagen'), 1), 'Europe/Copenhagen'),
    interval 1 hour
  )) as hour,
  _input.date as _date,
  _input.quantity as _quantity
  FROM _input
)
SELECT 
  hour,
  CASE WHEN hour = to_utc_timestamp(FROM_UTC_TIMESTAMP(_date, 'Europe/Copenhagen'), 'Europe/Copenhagen')
    THEN _quantity
    ELSE 0
   END AS quantity 
FROM hours
