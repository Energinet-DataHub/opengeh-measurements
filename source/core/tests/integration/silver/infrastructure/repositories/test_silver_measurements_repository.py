from geh_common.domain.types.orchestration_type import OrchestrationType
from pyspark.sql import SparkSession

import tests.helpers.datetime_helper as datetime_helper
import tests.helpers.identifier_helper as identifier_helper
import tests.helpers.table_helper as table_helper
from core.settings.silver_settings import SilverSettings
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.infrastructure.config import SilverTableNames
from core.silver.infrastructure.repositories.silver_measurements_repository import SilverMeasurementsRepository
from tests.helpers.builders.silver_measurements_builder import SilverMeasurementsBuilder


def test__append_if_not_exists__when_row_already_exists_in_table__should_not_append(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    )
    table_helper.append_to_table(
        silver_measurements,
        silver_settings.silver_database_name,
        SilverTableNames.silver_measurements,
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Assert
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 1


def test__append_if_not_exists__when_not_exists_in_table__should_append(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark).add_row(orchestration_instance_id=orchestration_instance_id).build()
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Assert
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 1


def test__append_if_not_exists__when_only_created_col_is_different__should_not_append(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    expected_created = datetime_helper.get_datetime(year=2020, month=1)
    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(orchestration_instance_id=orchestration_instance_id, created=expected_created)
        .build()
    )
    table_helper.append_to_table(
        silver_measurements,
        silver_settings.silver_database_name,
        SilverTableNames.silver_measurements,
    )

    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            created=datetime_helper.get_datetime(year=2020, month=2),
        )
        .build()
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Assert
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 1
    assert actual.collect()[0][SilverMeasurementsColumnNames.created].replace(tzinfo=None) == expected_created.replace(
        tzinfo=None
    )


def test__append_if_not_exists__when_data_exists_but_no_duplicates__should_append(spark: SparkSession) -> None:
    # Arrange
    silver_settings = SilverSettings()
    orchestration_instance_id = identifier_helper.generate_random_string()
    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            metering_point_id=identifier_helper.create_random_metering_point_id(),
        )
        .build()
    )
    table_helper.append_to_table(
        silver_measurements,
        silver_settings.silver_database_name,
        SilverTableNames.silver_measurements,
    )

    silver_measurements = (
        SilverMeasurementsBuilder(spark)
        .add_row(
            orchestration_instance_id=orchestration_instance_id,
            metering_point_id=identifier_helper.create_random_metering_point_id(),
        )
        .build()
    )

    # Act
    SilverMeasurementsRepository().append_if_not_exists(silver_measurements)

    # Assert
    actual = spark.table(f"{silver_settings.silver_database_name}.{SilverTableNames.silver_measurements}").where(
        f"orchestration_instance_id = '{orchestration_instance_id}'"
    )
    assert actual.count() == 2


def test__read_submitted_transaction__returns_expected(spark: SparkSession) -> None:
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
        SilverMeasurementsRepository()
        .read()
        .writeStream.format("delta")
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(assert_batch)
        .start()
        .awaitTermination()
    )

    # Assert
    assert assertion_count == 1
