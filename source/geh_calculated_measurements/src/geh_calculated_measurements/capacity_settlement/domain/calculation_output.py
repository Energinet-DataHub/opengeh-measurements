from dataclasses import dataclass

from geh_calculated_measurements.capacity_settlement.domain import Calculations, TenLargestQuantities
from geh_calculated_measurements.common.domain import CalculatedMeasurements


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    calculated_measurements: CalculatedMeasurements

    calculations: Calculations

    ten_largest_quantities: TenLargestQuantities
