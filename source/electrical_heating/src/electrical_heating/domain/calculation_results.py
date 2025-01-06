from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class CalculationOutput:
    """
    Contains the output of a calculation.
    """

    daily_child_consumption_with_limit: DataFrame | None = None

    calculation: DataFrame | None = None
