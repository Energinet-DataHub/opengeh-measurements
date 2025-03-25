# Migrated Transactions Bronze to Silver Transformation

When data goes from Bronze to Silver, we want to adapt our migrations data onto our measurements schema. This should be very similar, and most are just simple renamings, but the following columns had to have something changed.

## Table of transformed columns

| Column Name               | Description                                                                                                                                                                                                                                        | Transformations                                      | Implemented |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|-------------|
| orchestration_type        | When merging with submitted, we add "migration" as an enum here to avoid concurrent writes in silver. This is a new column that is added.                                                                                                          | Always set as "migration"                            | ✓           |
| orchestration_instance_id | When merging with submitted, we add a fully-zero UUID "00000000-0000-0000-0000-000000000000" as the value for all transactions.                                                                                                                    | Always set as "00000000-0000-0000-0000-000000000000" | ✓           |
| points                    | Renamed values, but due to contract in measurements the internal struct has it's order changed to (position, quantity, quality) whereas migrations used (position, quality, quantity). Also, quantity changed to Decimal(18,3) from Decimal(18,6). | Change ordering, and type of quantity.               | ✓           |
| is_cancelled              | Same check as in migrations, which is read_reason == "CAN" as a boolean column.                                                                                                                                                                    | Boolean read_reason == "CAN"                         | ✓           |
| is_deleted                | Same check as in migrations, which is status == 9 as a boolean column.                                                                                                                                                                             | Boolean status == 9                                  | ✓           |
| metering_point_type       | Mapping metering point type values used in Migrations to the values used in Measurements.                                                                                                                                                          | See [metering point type mapping](#metering-point-type-mapping)                 | ✓           |
| unit                      | Mapping unit values used in Migrations to the values used in Measurements.                                                                                                                                                                         | See [unit mapping](#unit-mapping)                    | ✓           |

### Metering Point Type mapping

| Migrations value | Measurements value             |
|------------------|--------------------------------|
| E17              | consumption                    |
| E18              | production                     |
| E20              | exchange                       |
| D01              | ve_production                  |
| D02              | analysis                       |
| D03              | not_used                       |
| D04              | surplus_production_group_6     |
| D05              | net_production                 |
| D06              | supply_to_grid                 |
| D07              | consumption_from_grid          |
| D08              | wholesale_services_information |
| D09              | own_production                 |
| D10              | net_from_grid                  |
| D11              | net_to_grid                    |
| D12              | total_consumption              |
| D13              | net_loss_correction            |
| D14              | electrical_heating             |
| D15              | net_consumption                |
| D17              | other_consumption              |
| D18              | other_production               |
| D19              | capacity_settlement            |
| D20              | exchange_reactive_energy       |
| D21              | collective_net_production      |
| D22              | collective_net_consumption     |
| D99              | internal_use                   |

### Unit mapping

| Migrations value | Measurements value |
|------------------|--------------------|
| KVARH            | KVARH              |
| KW               | KW                 |
| KWH              | KWH                |
| MWH              | MHW                |
| T                | TONNE              |
