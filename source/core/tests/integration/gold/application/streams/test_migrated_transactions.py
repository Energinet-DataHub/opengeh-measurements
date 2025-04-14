from datetime import datetime

from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.bronze.infrastructure.config.table_names import TableNames as BronzeTableNames
from core.gold.application.streams import migrated_transactions_stream as mit
from core.gold.infrastructure.config import GoldTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.gold_settings import GoldSettings
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder


def test__migrated_transactions__should_save_in_gold_measurements(
    mock_checkpoint_path, spark: SparkSession, migrations_executed, mocker: MockerFixture
) -> None:
    # Arrange
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    expected_transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark).add_row(transaction_id=expected_transaction_id)

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )

    # Act
    mit.stream_migrated_transactions_to_gold()

    # Assert
    gold_table = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert gold_table.count() == 24


def test__migrated_transactions__should_not_save_transactions_from_before_2017_in_gold_measurements(
    mock_checkpoint_path, spark: SparkSession, migrations_executed, mocker: MockerFixture
) -> None:
    # Arrange
    mocker.patch(f"{mit.__name__}.spark_session.initialize_spark", return_value=spark)

    expected_transaction_id = identifier_helper.generate_random_string()
    bronze_migrated_transactions = MigratedTransactionsBuilder(spark)
    bronze_migrated_transactions.add_row(
        transaction_id=expected_transaction_id,
        valid_from_date=datetime(2016, 12, 30, 23, 0, 0),
        valid_to_date=datetime(2016, 12, 31, 23, 0, 0),
    )

    table_helper.append_to_table(
        bronze_migrated_transactions.build(),
        BronzeSettings().bronze_database_name,
        BronzeTableNames.bronze_migrated_transactions_table,
    )

    # Act
    mit.stream_migrated_transactions_to_gold()

    # Assert
    gold_table = spark.table(f"{GoldSettings().gold_database_name}.{GoldTableNames.gold_measurements}").where(
        f"transaction_id = '{expected_transaction_id}'"
    )
    assert gold_table.count() == 0
