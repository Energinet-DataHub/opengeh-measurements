# Measurements Series SAP - Gold Table Definition

This table contains measurements series for SAP. Each row represents 1 series (transaction).

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| series_seq_no | DecimalType(14, 0) | True | - | Incremental number. See [`series_seq_no`](#series_seq_no) |
| orchestration_type | StringType | True | - | Valid values ["submitted", "migration", "electrical_heating", "capacity_settlement"] |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| start_date | TimestampType | True | | - |
| end_date | TimestampType | True | | - |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| is_cancelled | BooleanType | True | Cancelled flag carried over from Migrations | "is_cancelled_is_not_null_chk" which checks is_cancelled is not null,  |
| created | TimestampType | True | - | - |
| modified | TimestampType | True | - | - |

## `series_seq_no`

This number is unique for each row and is added to support how SAP works.

As SAP already have numbers, we will have to add a fixed number to our incremental `series_seq_no` to avoid conflicts.

For orchestration types: `submitted`, `electrical_heating` and `capacity_settlement` we have to add 200.000.000.000 (200 billions).
