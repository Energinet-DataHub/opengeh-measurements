from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    measurements: DataFrame | None = None

    calculations: DataFrame | None = None

    ten_largest_quantities: DataFrame | None = None
