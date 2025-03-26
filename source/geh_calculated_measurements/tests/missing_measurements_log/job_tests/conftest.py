import pytest
from geh_common.testing.delta_lake.delta_lake_operations import create_database, create_table
from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.domain import MeteringPointPeriods
from geh_calculated_measurements.missing_measurements_log.infrastructure.database_definitions import (
    MeteringPointPeriodsDatabaseDefinition,
)


@pytest.fixture(scope="session")
def metering_point_periods_table_created(spark: SparkSession) -> None:
    create_database(spark, MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME)

    create_table(
        spark,
        database_name=MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME,
        table_name=MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS,
        schema=MeteringPointPeriods.schema,
        table_location=f"{MeteringPointPeriodsDatabaseDefinition.DATABASE_NAME}/{MeteringPointPeriodsDatabaseDefinition.METERING_POINT_PERIODS}",
    )
