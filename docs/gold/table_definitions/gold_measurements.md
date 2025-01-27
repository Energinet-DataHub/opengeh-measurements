# Measurements - Gold Table Definition

This table contains measurements which is presented as one measurement value per metering point per row. The table depends on `silver.measurements`.

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| observation_time | TimestampType | True | The time when the energy was consumed/produced/exchanged | - |
| quantity | Decimal(18, 3) | True | The energy quantity. Negative values allowed. May be null when the quality is 'missing' | - |
| quality | StringType | True | The quality of the energy quantity. | - |
| metering_point_type | StringType | True | - | - |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| created | TimestampType | True | - | - |
| modified | TimestampType | True | - | - |
