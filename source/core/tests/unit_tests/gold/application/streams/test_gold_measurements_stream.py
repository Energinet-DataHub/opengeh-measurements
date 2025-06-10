import os
from unittest.mock import ANY, Mock

from pytest_mock import MockFixture

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.gold.infrastructure.repositories.measurements_sap_series_repository import GoldMeasurementsSAPSeriesRepository
from core.receipts.infrastructure.repositories.receipts_repository import ReceiptsRepository


def test__stream_measurements_silver_to_gold__calls_expected(mocker: MockFixture):
    # Arrange
    silver_repo_mock = mocker.patch.object(sut, sut.SilverMeasurementsRepository.__name__)
    gold_stream_mock = mocker.patch.object(sut, sut.GoldMeasurementsStream.__name__)

    # Act
    sut.stream_measurements_silver_to_gold()

    # Assert
    silver_repo_mock.assert_called_once()
    gold_stream_mock.assert_called_once()


def test__pipeline_measurements_silver_to_gold__calls_append_to_gold_measurements(mocker: MockFixture):
    # Arrange
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    receipts_repo_mock = Mock(spec=ReceiptsRepository)
    series_sap_repo_mock = Mock(spec=GoldMeasurementsSAPSeriesRepository)
    transform_mock = Mock()
    transform_receipts_mock = Mock()
    transform_sap_series_mock = Mock()
    mocker.patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock)
    mocker.patch.object(sut.transformations, "transform_silver_to_gold", transform_mock)
    mocker.patch.object(sut, "ReceiptsRepository", return_value=receipts_repo_mock)
    mocker.patch.object(sut.receipt_transformations, "transform", transform_receipts_mock)
    mocker.patch.object(sut, "GoldMeasurementsSAPSeriesRepository", return_value=series_sap_repo_mock)
    mocker.patch.object(sut.sap_series_transformations, "transform", transform_sap_series_mock)
    silver_measurements_mock = Mock()

    # Act
    sut._batch_operation(silver_measurements_mock, 0)

    # Assert
    transform_mock.assert_called_once_with(silver_measurements_mock)
    gold_repo_mock.append_if_not_exists.assert_called_once_with(
        transform_mock.return_value, query_name=QueryNames.SILVER_TO_GOLD
    )
    transform_receipts_mock.assert_called_once_with(ANY)
    receipts_repo_mock.append_if_not_exists.assert_called_once_with(ANY)
    transform_sap_series_mock.assert_called_once_with(ANY)
    series_sap_repo_mock.append_if_not_exists.assert_called_once_with(ANY)


def test__pipeline_measurements_silver_to_gold__when_stream_submitted_to_sap_series_is_false__does_not_call_series_sap(
    mocker: MockFixture,
):
    # Arrange
    os.environ["STREAM_SUBMITTED_TO_SAP_SERIES"] = "false"
    gold_repo_mock = Mock(spec=GoldMeasurementsRepository)
    receipts_repo_mock = Mock(spec=ReceiptsRepository)
    sap_series_repo_mock = Mock(spec=GoldMeasurementsSAPSeriesRepository)
    transform_mock = Mock()
    transform_receipts_mock = Mock()
    transform_sap_series_mock = Mock()
    mocker.patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock)
    mocker.patch.object(sut.transformations, "transform_silver_to_gold", transform_mock)
    mocker.patch.object(sut, "ReceiptsRepository", return_value=receipts_repo_mock)
    mocker.patch.object(sut.receipt_transformations, "transform", transform_receipts_mock)
    mocker.patch.object(sut, "GoldMeasurementsSAPSeriesRepository", return_value=sap_series_repo_mock)
    mocker.patch.object(sut.sap_series_transformations, "transform", transform_sap_series_mock)
    silver_measurements_mock = Mock()

    # Act
    sut._batch_operation(silver_measurements_mock, 0)

    # Assert
    transform_sap_series_mock.assert_not_called()
    sap_series_repo_mock.append_if_not_exists.assert_not_called()
