from unittest.mock import ANY, Mock

from pytest_mock import MockFixture

import core.gold.application.streams.gold_measurements_stream as sut
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.repositories.gold_measurements_repository import GoldMeasurementsRepository
from core.gold.infrastructure.repositories.measurements_series_sap_repository import GoldMeasurementsSeriesSAPRepository
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
    series_sap_repo_mock = Mock(spec=GoldMeasurementsSeriesSAPRepository)
    transform_mock = Mock()
    transform_receipts_mock = Mock()
    transform_series_sap_mock = Mock()
    mocker.patch.object(sut, "GoldMeasurementsRepository", return_value=gold_repo_mock)
    mocker.patch.object(sut.transformations, "transform_silver_to_gold", transform_mock)
    mocker.patch.object(sut, "ReceiptsRepository", return_value=receipts_repo_mock)
    mocker.patch.object(sut.receipt_transformations, "transform", transform_receipts_mock)
    mocker.patch.object(sut, "GoldMeasurementsSeriesSAPRepository", return_value=series_sap_repo_mock)
    mocker.patch.object(sut.series_sap_transformations, "transform", transform_series_sap_mock)
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
    transform_series_sap_mock.assert_called_once_with(ANY)
    series_sap_repo_mock.append_if_not_exists.assert_called_once_with(ANY)
