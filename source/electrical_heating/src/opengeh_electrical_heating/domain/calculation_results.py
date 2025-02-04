from dataclasses import dataclass

from pyspark.sql import DataFrame

from opengeh_calculated_measurements.opengeh_electrical_heating.domain.calculated_measurements_daily import (
    CalculatedMeasurementsDaily,
)


@dataclass
class CalculationOutput:
    """Contains the output of a calculation."""

    measurements: CalculatedMeasurementsDaily

    calculations: DataFrame
