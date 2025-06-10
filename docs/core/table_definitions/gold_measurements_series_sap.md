# Measurements Series SAP - Gold Table Definition

This table contains measurements series for SAP. Each row represents 1 series (transaction).

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| orchestration_type | StringType | True | - | Valid values ["submitted", "migration", "electrical_heating", "capacity_settlement"] |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| start_date | TimestampType | True | | - |
| end_date | TimestampType | True | | - |
| unit | StringType | True | - | Not null |
| resolution | StringType | True | - | Not null |
| created | TimestampType | True | - | - |
