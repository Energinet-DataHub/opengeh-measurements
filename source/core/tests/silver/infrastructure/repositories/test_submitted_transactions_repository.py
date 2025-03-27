from unittest.mock import Mock

from geh_common.domain.types.orchestration_type import OrchestrationType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import SparkSession

import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.repositories.submitted_transactions_repository import SubmittedTransactionsRepository
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__read_submitted_transactions__calls_expected() -> None:
    # Arrange
    mock_spark = Mock()
    expected_table = f"{SilverSettings().silver_database_name}.{SilverTableNames.silver_measurements}"

    # Act
    _ = SubmittedTransactionsRepository(mock_spark).read_submitted_transactions()

    # Assert
    mock_spark.readStream.format.assert_called_once_with("delta")
    mock_spark.readStream.format().option.assert_called_once_with("ignoreDeletes", "true")
    mock_spark.readStream.format().option().option.assert_called_once_with("skipChangeCommits", "true")
    mock_spark.readStream.format().option().option().table.assert_called_once_with(expected_table)
    mock_spark.readStream.format().option().option().table().filter.assert_called_once_with(
        f"{SilverMeasurementsColumnNames.orchestration_type} = '{GehCommonOrchestrationType.SUBMITTED.value}'"
    )


def test__read_submitted_transaction__returns_expected(spark: SparkSession, migrations_executed) -> None:
    # Arrange
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id, orchestration_type=OrchestrationType.SUBMITTED.value
        )
        .build()
    )

    table_helper.append_to_table(
        silver_measurements, SilverSettings().silver_database_name, SilverTableNames.silver_measurements
    )

    def assert_batch(batch_df, _):
        global assertion_count
        actual = batch_df.where(
            f"{SilverMeasurementsColumnNames.orchestration_instance_id} = '{orchestration_instance_id}'"
        )
        assertion_count = actual.count()

    # Act
    (
        SubmittedTransactionsRepository(spark)
        .read_submitted_transactions()
        .writeStream.format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(assert_batch)
        .start()
        .awaitTermination()
    )

    # Assert
    assert assertion_count == 1
