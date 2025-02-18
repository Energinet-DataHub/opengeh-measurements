from pyspark.sql import SparkSession

import core.silver.application.streams.submitted_transactions as sut
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.catalog_settings import CatalogSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.submitted_transactions_builder import SubmittedTransactionsBuilder, ValueBuilder

# @mock.patch("core.silver.application.streams.submitted_transactions.spark_session.initialize_spark")
# @mock.patch("core.silver.application.streams.submitted_transactions.BronzeRepository")
# @mock.patch(
#     "core.silver.application.streams.submitted_transactions.submitted_transactions_transformation.create_by_packed_submitted_transactions"
# )
# @mock.patch(
#     "core.silver.application.streams.submitted_transactions.measurements_transformation.create_by_unpacked_submitted_transactions"
# )
# @mock.patch("core.silver.application.streams.submitted_transactions.SilverRepository")
# def test__submitted_transactions__should_call_expected(
#     mock_SilverRepository,
#     mock_measurements_transformation_create_by_submitted_transactions,
#     mock_create_by_packed_submitted_transactions,
#     mock_BronzeRepository,
#     mock_initialize_spark,
# ) -> None:
#     # Arrange
#     mock_spark = mock.Mock()
#     mock_initialize_spark.return_value = mock_spark
#     mock_submitted_transactions = mock.Mock()
#     mock_BronzeRepository.return_value = mock_submitted_transactions
#     mock_unpacked_submitted_transactions = mock.Mock()
#     mock_create_by_packed_submitted_transactions.return_value = mock_unpacked_submitted_transactions
#     mock_measurements = mock.Mock()
#     mock_measurements_transformation_create_by_submitted_transactions.return_value = mock_measurements
#     mock_write_measurements = mock.Mock()
#     mock_SilverRepository.return_value = mock_write_measurements

#     # Act
#     sut.stream_submitted_transactions()

#     # Assert
#     mock_initialize_spark.assert_called_once()
#     mock_BronzeRepository.assert_called_once()
#     mock_create_by_packed_submitted_transactions.assert_called_once()
#     mock_measurements_transformation_create_by_submitted_transactions.assert_called_once()
#     mock_SilverRepository.assert_called_once()


def test__submitted_transactions__should_save_in_silver_measurements(spark: SparkSession, migrate) -> None:
    # Arrange
    catalog_settings = CatalogSettings()  # type: ignore
    expected_orchestration_id = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=expected_orchestration_id).build()
    submitted_transactions = SubmittedTransactionsBuilder(spark).add_row(value=value).build()
    table_helper.append_to_table(
        submitted_transactions,
        catalog_settings.bronze_database_name,
        BronzeTableNames.bronze_submitted_transactions_table,
    )

    # Act
    sut.stream_submitted_transactions()

    # Assert
    silver_table = spark.table(f"{catalog_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{expected_orchestration_id}'"
    )
    assert silver_table.count() == 1
