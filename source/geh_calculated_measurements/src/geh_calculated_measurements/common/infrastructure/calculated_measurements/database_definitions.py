class CalculatedMeasurementsInternalDatabaseDefinition:
    """Database with internal tables for calculated measurements."""

    DATABASE_NAME = "measurements_calculated_internal"
    """Should match whatever name is currently being used in Databricks"""

    # Table names
    MEASUREMENTS_TABLE_NAME = "calculated_measurements"
    CAPACITY_SETTLEMENT_TEN_LARGEST_QUANTITIES_TABLE_NAME = "capacity_settlement_ten_largest_quantities"
    CAPACITY_SETTLEMENT_CALCULATIONS_TABLE_NAME = "capacity_settlement_calculations"
    CALCULATED_ESTIMATED_ANNUAL_CONSUMPTION_TABLE_NAME = "calculated_estimated_annual_consumption"


class CalculatedMeasurementsDatabaseDefinition:
    """Database with data products for calculated measurements."""

    DATABASE_NAME = "measurements_calculated"

    HOURLY_CALCULATED_MEASUREMENTS_VIEW_NAME = "hourly_calculated_measurements_v1"
