﻿from enum import Enum


class MeteringPointType(Enum):
    SUPPLY_TO_GRID = "supply_to_grid"
    CONSUMPTION_FROM_GRID = "consumption_from_grid"
    ELECTRICAL_HEATING = "electrical_heating"
    NET_CONSUMPTION = "net_consumption"