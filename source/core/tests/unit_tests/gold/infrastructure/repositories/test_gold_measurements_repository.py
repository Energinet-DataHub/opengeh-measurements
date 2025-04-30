from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.gold.infrastructure.repositories.gold_measurements_repository as sut
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from tests.helpers.builders.gold_builder import GoldMeasurementsBuilder


def test__append_if_not_exists__when_silver_to_gold__calls_expected(spark: SparkSession, mocker: MockerFixture) -> None:
    # Arrange
    gold_measurements = GoldMeasurementsBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsColumnNames.metering_point_id,
        sut.GoldMeasurementsColumnNames.orchestration_type,
        sut.GoldMeasurementsColumnNames.observation_time,
        sut.GoldMeasurementsColumnNames.transaction_id,
        sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
    ]

    # Act
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements, QueryNames.SILVER_TO_GOLD)

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
            sut.GoldMeasurementsColumnNames.observation_time,
        ],
        target_filters={
            sut.GoldMeasurementsColumnNames.orchestration_type: [sut.GehCommonOrchestrationType.SUBMITTED.value]
        },
    )


def test__append_if_not_exists__when_migrations_to_gold__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    gold_measurements = GoldMeasurementsBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsColumnNames.metering_point_id,
        sut.GoldMeasurementsColumnNames.observation_time,
        sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
        sut.GoldMeasurementsColumnNames.transaction_id,
        sut.GoldMeasurementsColumnNames.resolution,
        sut.GoldMeasurementsColumnNames.is_cancelled,
    ]

    # Act
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements, QueryNames.MIGRATIONS_TO_GOLD)

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
            sut.GoldMeasurementsColumnNames.observation_time,
        ],
        target_filters={
            sut.GoldMeasurementsColumnNames.orchestration_type: [sut.GehCommonOrchestrationType.MIGRATION.value]
        },
    )


def test__append_if_not_exists__when_calculated_to_gold__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    gold_measurements = GoldMeasurementsBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsColumnNames.metering_point_id,
        sut.GoldMeasurementsColumnNames.orchestration_type,
        sut.GoldMeasurementsColumnNames.observation_time,
        sut.GoldMeasurementsColumnNames.transaction_id,
        sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
    ]

    # Act
    GoldMeasurementsRepository().append_if_not_exists(gold_measurements, QueryNames.CALCULATED_TO_GOLD)

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsColumnNames.transaction_creation_datetime,
            sut.GoldMeasurementsColumnNames.observation_time,
        ],
        target_filters={
            sut.GoldMeasurementsColumnNames.orchestration_type: [
                sut.GehCommonOrchestrationType.CAPACITY_SETTLEMENT.value,
                sut.GehCommonOrchestrationType.ELECTRICAL_HEATING.value,
                sut.GehCommonOrchestrationType.NET_CONSUMPTION.value,
            ]
        },
    )
