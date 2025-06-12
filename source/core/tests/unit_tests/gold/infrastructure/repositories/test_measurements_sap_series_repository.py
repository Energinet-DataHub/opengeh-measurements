from pyspark.sql import SparkSession
from pytest_mock import MockerFixture

import core.gold.infrastructure.repositories.measurements_sap_series_repository as sut
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.measurements_sap_series_repository import GoldMeasurementsSAPSeriesRepository
from tests.helpers.builders.gold_sap_series_builder import GoldSAPSeriesBuilder


def test__append_if_not_exists__when_silver_to_gold__calls_expected(spark: SparkSession, mocker: MockerFixture) -> None:
    # Arrange
    gold_measurements = GoldSAPSeriesBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type,
        sut.GoldMeasurementsSAPSeriesColumnNames.metering_point_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime,
        sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        sut.GoldMeasurementsSAPSeriesColumnNames.resolution,
    ]

    # Act
    GoldMeasurementsSAPSeriesRepository().append_if_not_exists(gold_measurements, QueryNames.SILVER_TO_GOLD_SAP_SERIES)

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsSAPSeriesRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        ],
        target_filters={
            sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [
                sut.GehCommonOrchestrationType.SUBMITTED.value
            ]
        },
    )


def test__append_if_not_exists__when_migrations_to_gold__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    gold_measurements = GoldSAPSeriesBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type,
        sut.GoldMeasurementsSAPSeriesColumnNames.metering_point_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime,
        sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        sut.GoldMeasurementsSAPSeriesColumnNames.resolution,
    ]

    # Act
    GoldMeasurementsSAPSeriesRepository().append_if_not_exists(
        gold_measurements, QueryNames.MIGRATIONS_TO_SAP_SERIES_GOLD
    )

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsSAPSeriesRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        ],
        target_filters={
            sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [
                sut.GehCommonOrchestrationType.MIGRATION.value
            ]
        },
    )


def test__append_if_not_exists__when_calculated_to_gold__calls_expected(
    spark: SparkSession, mocker: MockerFixture
) -> None:
    # Arrange
    gold_measurements = GoldSAPSeriesBuilder(spark).add_row().build()
    mocked_delta_table_helper = mocker.patch(
        f"{sut.__name__}.delta_table_helper",
    )
    expected_merge_columns = [
        sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type,
        sut.GoldMeasurementsSAPSeriesColumnNames.metering_point_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_id,
        sut.GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime,
        sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        sut.GoldMeasurementsSAPSeriesColumnNames.resolution,
    ]

    # Act
    GoldMeasurementsSAPSeriesRepository().append_if_not_exists(
        gold_measurements, QueryNames.CALCULATED_TO_GOLD_SAP_SERIES
    )

    # Arrange
    mocked_delta_table_helper.append_if_not_exists.assert_called_with(
        spark,
        gold_measurements,
        GoldMeasurementsSAPSeriesRepository().table,
        expected_merge_columns,
        clustering_columns_to_filter_specifically=[
            sut.GoldMeasurementsSAPSeriesColumnNames.start_time,
        ],
        target_filters={
            sut.GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [
                sut.GehCommonOrchestrationType.CAPACITY_SETTLEMENT.value,
                sut.GehCommonOrchestrationType.ELECTRICAL_HEATING.value,
                sut.GehCommonOrchestrationType.NET_CONSUMPTION.value,
            ]
        },
    )
