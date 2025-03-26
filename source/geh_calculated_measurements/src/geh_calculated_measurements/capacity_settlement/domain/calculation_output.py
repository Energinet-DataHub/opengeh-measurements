from dataclasses import dataclass

from pyspark.sql import DataFrame

from geh_calculated_measurements.capacity_settlement.domain import TenLargestQuantities


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    calculated_measurements_daily: DataFrame

    ten_largest_quantities: TenLargestQuantities
