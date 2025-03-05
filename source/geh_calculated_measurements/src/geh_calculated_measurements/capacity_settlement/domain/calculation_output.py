from dataclasses import dataclass

from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import CalculatedMeasurements, TenLargestQuantities


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    calculated_measurements: CalculatedMeasurements

    calculations: DataFrame

    ten_largest_quantities: TenLargestQuantities
