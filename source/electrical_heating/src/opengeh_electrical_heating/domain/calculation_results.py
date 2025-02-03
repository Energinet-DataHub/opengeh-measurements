from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    measurements: DataFrame

    calculations: DataFrame
