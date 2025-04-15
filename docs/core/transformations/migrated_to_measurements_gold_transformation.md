# Migrated Transactions Bronze to Gold Transformation

When data goes from Bronze directly to Gold, we want to adapt the data from the Migrations subsystem onto the Measurements schema. This should be very similar, and most are just simple renamings, but the following columns had to have something changed.

## Table of transformed columns

| Column Name                | Description                                                                                                                                                                                                 | Transformations                                      | Implemented |
|----------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------|-------------|
| orchestration_type         | When merging with gold, we add "migration" as an enum here to avoid concurrent writes in gold. This is a new column that is added.                                                                          | Always set as "migration"                            | ✓           |
| orchestration_instance_id  | When merging with gold, we add a fully-zero UUID "00000000-0000-0000-0000-000000000000" as the value for all transactions.                                                                                  | Always set as "00000000-0000-0000-0000-000000000000" | ✓           |
| is_cancelled + is_deleted  | Both may impact gold's is_cancelled                                                                                                                                                                         | True when `read_reason == "CAN"` OR `status == 9`    | ✓           |
| metering_point_type        | Mapping metering point type values used in the Migrations subsystem to the values used in Measurements.                                                                                                     | See [metering point type mapping](#metering-point-type-mapping)  | ✓           |
| unit                       | Mapping unit values used in the Migrations subsystem to the values used in Measurements.                                                                                                                    | See [unit mapping](#unit-mapping)                    | ✓           |

### Metering Point Type mapping

| Migrations subsystem | Measurements subsystem         |
|----------------------|--------------------------------|
| E17                  | consumption                    |
| E18                  | production                     |
| E20                  | exchange                       |
| D01                  | ve_production                  |
| D02                  | analysis                       |
| D03                  | not_used                       |
| D04                  | surplus_production_group_6     |
| D05                  | net_production                 |
| D06                  | supply_to_grid                 |
| D07                  | consumption_from_grid          |
| D08                  | wholesale_services_information |
| D09                  | own_production                 |
| D10                  | net_from_grid                  |
| D11                  | net_to_grid                    |
| D12                  | total_consumption              |
| D13                  | net_loss_correction            |
| D14                  | electrical_heating             |
| D15                  | net_consumption                |
| D17                  | other_consumption              |
| D18                  | other_production               |
| D19                  | capacity_settlement            |
| D20                  | exchange_reactive_energy       |
| D21                  | collective_net_production      |
| D22                  | collective_net_consumption     |
| D99                  | internal_use                   |

### Unit mapping

| Migrations subsystem | Measurements subsystem |
|----------------------|------------------------|
| KVARH                | kVArh                  |
| KW                   | kW                     |
| KWH                  | kWh                    |
| MWH                  | MWh                    |
| T                    | Tonne                  |
