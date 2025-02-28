# Measurements - Silver Table Definition

| Column name | Data type | Nullable | Description | Constraints |
| - | - | - | - | - |
| orchestration_type | StringType | True | - | Valid values ["submitted", "migration"] |
| orchestration_instance_id | StringType | True | - | - |
| metering_point_id | StringType | True | The GSRN number that uniquely identifies the metering point | Exactly 18 digits |
| transaction_id | StringType | True | Contains an ID for the specific time series transaction, provided by the sender or the source system. Uniqueness not guaranteed | - |
| transaction_creation_datetime | TimestampType | True | Contains the UTC time for when the time series data was persisted in source system | - |
| metering_point_type | StringType | True | - | Valid [metering point type](https://github.com/Energinet-DataHub/opengeh-python-packages/blob/main/src/geh_common/domain/types/metering_point_type.py) values |
| unit | StringType | True | - | Valid values ["KWH", "KW", "KVARH", "MWH", "TONNE"], we need to extend this [list](https://github.com/Energinet-DataHub/opengeh-python-packages/blob/main/src/geh_common/domain/types/quantity_unit.py) |
| resolution | StringType | True | - | Valid values ["PT15M", "PT1H", "P1M"], we need to add new file to python packages |
| start_datetime | TimestampType | True | - | - |
| end_datetime | TimestampType | True | - | - |
| points[*].position | StringType | True | - | - |
| points[*].quantity | Decimal(18, 3) | True | The energy quantity. Negative values allowed. May be null when the quality is 'missing' | - |
| points[*].quality | StringType | True | The quality of the energy quantity. | Transformation to e.g. "measured" will be handled towards gold |
| created | TimestampType | True | - | - |
