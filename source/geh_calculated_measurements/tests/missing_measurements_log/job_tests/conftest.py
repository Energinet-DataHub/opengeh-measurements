import pytest
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain import CurrentMeasurements
from geh_calculated_measurements.common.infrastructure.current_measurements.database_definitions import (
    MeasurementsGoldDatabaseDefinition,
)
from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)


@pytest.fixture(scope="session")
def input_tables_created(spark: SparkSession) -> None:
    create_database(spark, MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME,
        table_name=MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS,
        schema=MeteringPointPeriods.schema,
        table_location=f"{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}/{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}",
    )

    create_database(spark, MeasurementsGoldDatabaseDefinition.DATABASE_NAME)
    create_table(
        spark,
        database_name=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
        table_name=MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS,
        schema=CurrentMeasurements.schema,
        table_location=f"{MeasurementsGoldDatabaseDefinition.DATABASE_NAME}/{MeasurementsGoldDatabaseDefinition.CURRENT_MEASUREMENTS}",
    )
