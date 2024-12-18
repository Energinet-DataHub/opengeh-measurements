from enum import Enum


class MeteringPointType(Enum):
    CONSUMPTION_FROM_GRID = "consumption_from_grid"
    ELECTRICAL_HEATING = "electrical_heating"
    NET_CONSUMPTION = "net_consumption"
    SUPPLY_TO_GRID = "supply_to_grid"
