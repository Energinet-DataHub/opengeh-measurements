import pytest


@pytest.mark.skip(reason="MeasurementsRepository.read_time_series_points: the table does not yet exist in the database")
def test__when_missing_expected_column_raises_exception() -> None:
    pass


@pytest.mark.skip(reason="MeasurementsRepository.read_time_series_points: the table does not yet exist in the database")
def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column() -> None:
    pass


@pytest.mark.skip(reason="MeasurementsRepository.read_time_series_points: the table does not yet exist in the database")
def test__when_source_contains_wrong_data_type_raises_exception() -> None:
    pass
