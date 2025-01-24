# Measurements - Silver Table Definition

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| orchestration_type | StringType | True | - | - |
| orchestration_instance_id | StringType | True | - | - |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| metering_point_type | StringType | True | - | - |
| product | StringType | True | - | - |
| unit | StringType | True | - | - |
| resolution | StringType | True | - | - |
| start_datetime | TimestampType | True | - | - |
| end_datetime | TimestampType | True | - | - |
| points[*].position | StringType | True | - | - |
| points[*].quantity | Decimal(18, 3) | True | The energy quantity. Negative values allowed. May be null when the quality is 'missing' | - |
| points[*].quality | StringType | True | The quality of the energy quantity. | - |
| created | TimestampType | True | - | - |
