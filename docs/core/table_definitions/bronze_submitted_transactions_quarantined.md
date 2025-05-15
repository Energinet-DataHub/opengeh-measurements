# Measurements - Bronze Submitted Transactions Table Definitions

This tables contains quarantined transactions. The reason that a row have been quarantined can be found in the columns prefixed with: `validate_`. When the value is `false`, the validation failed.

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| orchestration_type | StringType | True | - | Valid values ["submitted", "migration"] |
| orchestration_instance_id | StringType | True | - | - |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| metering_point_type | StringType | True | - | Valid [metering point type](https://github.com/Energinet-DataHub/opengeh-python-packages/blob/main/source/geh_common/src/geh_common/domain/types/metering_point_type.py) values |
| unit | StringType | True | - | Valid [unit types](https://github.com/Energinet-DataHub/opengeh-python-packages/blob/main/source/geh_common/src/geh_common/domain/types/quantity_unit.py) |
| resolution | StringType | True | - | Valid values ["PT15M", "PT1H", "P1M"], we need to add new file to python packages |
| start_datetime | TimestampType | True | - | - |
| end_datetime | TimestampType | True | - | - |
| points[*].position | StringType | True | - | - |
| points[*].quantity | Decimal(18, 3) | True | The energy quantity. Negative values allowed. May be null when the quality is 'missing' | - |
| points[*].quality | StringType | True | The quality of the energy quantity. | Valid [quality types](https://github.com/Energinet-DataHub/opengeh-python-packages/blob/main/source/geh_common/src/geh_common/domain/types/quantity_quality.py) |
| is_cancelled | BooleanType | True | Cancelled flag carried over from Migrations | "is_cancelled_is_not_null_chk" which checks is_cancelled is not null,  |
| created | TimestampType | True | - | - |
| validate_orchestration_type_enum | BooleanType | True | - | - |
| validate_quality_enum | BooleanType | True | - | - |
| validate_metering_point_type_enum | BooleanType | True | - | - |
| validate_unit_enum | BooleanType | True | - | - |
| validate_resolution_enum | BooleanType | True | - | - |
