from dataclasses import dataclass

from pandas import DataFrame


@dataclass
class CalculationOutput:
    """
    Contains the output of a calculation.
    """

    measurements: DataFrame | None = None
    calculations: DataFrame | None = None
