# Measurements - Silver Table Definition

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

## Transformations

| Column Name | Description | Transformation |
| - | - | - |
| Orchestration Type | Orchestration Type values, are prefixed with `OT_`, we'd like to align with `opengeh-python-packages` domain values. | `OT_SUBMITTED_MEASURE_DATA` -> `submitted` <br> Otherwise the value from the original data |
| Metering Point Type | Metering Point Type values, are prefixed with `MPT_`, we'd like to align this with `opengeh-python-packages` domain values. | `MPT_UNSPECIFIED` -> `MPT_UNSPECIFIED` <br> `MPT_ANALYSIS` -> `ANALYSIS` <br> `MPT_COLLECTIVE_NET_CONSUMPTION` -> `COLLECTIVE_NET_CONSUMPTION` <br> `MPT_COLLECTIVE_NET_PRODUCTION` -> `COLLECTIVE_NET_PRODUCTION` <br> `MPT_CONSUMPTION` -> `CONSUMPTION` <br> `MPT_CONSUMPTION_FROM_GRID` -> `CONSUMPTION_FROM_GRID` <br> `MPT_CAPACITY_SETTLEMENT` -> `CAPACITY_SETTLEMENT` <br> `MPT_ELECTRICAL_HEATING` -> `ELECTRICAL_HEATING` <br> `MPT_EXCHANGE` -> `EXCHANGE` <br> `MPT_EXCHANGE_REACTIVE_ENERGY` -> `EXCHANGE_REACTIVE_ENERGY` <br> `MPT_NET_CONSUMPTION` -> `NET_CONSUMPTION` <br> `MPT_NET_FROM_GRID` -> `NET_FROM_GRID` <br> `MPT_NET_LOSS_CORRECTION` -> `NET_LOSS_CORRECTION` <br> `MPT_NET_PRODUCTION` -> `NET_PRODUCTION` <br> `MPT_NET_TO_GRID` -> `NET_TO_GRID` <br> `MPT_NOT_USED` -> `NOT_USED` <br> `MPT_OTHER_CONSUMPTION` -> `OTHER_CONSUMPTION` <br> `MPT_OTHER_PRODUCTION` -> `OTHER_PRODUCTION` <br> `MPT_OWN_PRODUCTION` -> `OWN_PRODUCTION` <br> `MPT_PRODUCTION` -> `PRODUCTION` <br> `MPT_SUPPLY_TO_GRID` -> `SUPPLY_TO_GRID` <br> `MPT_SURPLUS_PRODUCTION_GROUP_6` -> `SURPLUS_PRODUCTION_GROUP_6` <br> `MPT_TOTAL_CONSUMPTION` -> `TOTAL_CONSUMPTION` <br> `MPT_VE_PRODUCTION` -> `VE_PRODUCTION` <br> `MPT_WHOLESALE_SERVICES_INFORMATION` -> `WHOLESALE_SERVICES_INFORMATION` <br> Otherwise the value from the original data. |
| Unit | Unit values, are prefixed with `U_`, we'd like to align this with `opengeh-python-packages` domain values. | `U_KWH` -> `kWh` <br> `U_KW` -> `kW` <br> `U_MWH` -> `MWh` <br> `U_TONNE` -> `Tonne` <br> `U_KVARH` -> `kVArh` <br> Otherwise the value from the original data |
| Resolution | Resolution values, are prefixed with `U_`, we'd like to align this with `opengeh-python-packages` domain values. | `R_PT15M`-> `PT15M` <br> `R_PT1H` -> `PT1H` <br> Otherwise the value from the original data |
