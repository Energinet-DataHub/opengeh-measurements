from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from geh_calculated_measurements.electrical_heating.infrastructure import ElectricityMarketRepository


@pytest.fixture(scope="session")
def test_files_folder_path() -> str:
    return str(Path(__file__).parent / "test_data")


@pytest.fixture(scope="session")
def electricity_market_repository_extra_col(
    spark: SparkSession, test_files_folder_path: str
) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        test_files_folder_path,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_extra_col.csv",
        child_metering_points_file_name="child_metering_points_v1_extra_col.csv",
    )


@pytest.fixture(scope="session")
def electricity_market_repository_missing_col(
    spark: SparkSession, test_files_folder_path: str
) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(
        spark,
        test_files_folder_path,
        consumption_metering_point_periods_file_name="consumption_metering_point_periods_v1_missing_col.csv",
        child_metering_points_file_name="child_metering_points_v1_missing_col.csv",
    )


@pytest.fixture(scope="session")
def electricity_market_repository(spark: SparkSession, test_files_folder_path: str) -> ElectricityMarketRepository:
    return ElectricityMarketRepository(spark, test_files_folder_path)
