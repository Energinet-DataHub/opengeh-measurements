from dataclasses import dataclass

from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import CalculatedMeasurements


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    measurements: CalculatedMeasurements

    calculations: DataFrame

    ten_largest_quantities: DataFrame
