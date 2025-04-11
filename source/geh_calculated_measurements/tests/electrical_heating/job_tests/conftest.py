import pytest
from geh_common.pyspark.read_csv import read_csv_path
from pyspark.sql import SparkSession

from tests.electrical_heating.job_tests import get_test_files_folder_path
from tests.external_data_products import ExternalDataProducts


@pytest.fixture(scope="session")
def gold_table_seeded(
    spark: SparkSession,
    external_dataproducts_created: None,  # Used implicitly
) -> None:
    database_name = ExternalDataProducts.CURRENT_MEASUREMENTS.database_name
    table_name = ExternalDataProducts.CURRENT_MEASUREMENTS.view_name
    schema = ExternalDataProducts.CURRENT_MEASUREMENTS.schema
    file_name = f"{get_test_files_folder_path()}/{database_name}-{table_name}.csv"
    time_series_points = read_csv_path(spark, file_name, schema)
    time_series_points.write.saveAsTable(
        f"{database_name}.{table_name}",
        format="delta",
        mode="overwrite",
    )
