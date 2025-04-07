from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.gold.application.streams import migrated_transactions_stream as mit
from core.settings.bronze_settings import BronzeSettings
from core.settings.silver_settings import SilverSettings
from core.silver.infrastructure.config import SilverTableNames
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder


def test__migrated_transactions__should_save_in_silver_measurements(
    mock_checkpoint_path, spark: SparkSession, migrations_executed, mocker: MockerFixture
) -> None:
    # Arrange
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)
    bronze_settings = BronzeSettings()
    silver_settings = SilverSettings()

    expected_transaction_id = "UnitTestingMigratedStream"
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=expected_transaction_id)

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        bronze_settings.bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )

    # Act
    mit.stream_migrated_transactions_to_gold()

    # Assert
    silver_table = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert silver_table.count() == 1
