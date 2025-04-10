from dataclasses import dataclass

from geh_calculated_measurements.capacity_settlement.domain import TenLargestQuantities
from geh_calculated_measurements.common.domain.model.calculated_measurements import CalculatedMeasurementsDaily


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    calculated_measurements_daily: CalculatedMeasurementsDaily

    ten_largest_quantities: TenLargestQuantities
