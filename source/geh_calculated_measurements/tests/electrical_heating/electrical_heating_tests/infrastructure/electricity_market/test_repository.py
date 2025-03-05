import pytest

from geh_calculated_measurements.electrical_heating.infrastructure import (
    ElectricityMarketRepository,
)


def test__when_missing_expected_column_raises_exception(
    electricity_market_repository_missing_col: ElectricityMarketRepository,
) -> None:
    with pytest.raises(ValueError, match=r"Column has_electrical_heating not found in CSV"):
        electricity_market_repository_missing_col.read_consumption_metering_point_periods()
    with pytest.raises(ValueError, match=r"Column metering_point_sub_type not found in CSV"):
        electricity_market_repository_missing_col.read_child_metering_points()


def test__when_source_contains_unexpected_columns_returns_data_without_unexpected_column(
    electricity_market_repository_extra_col: ElectricityMarketRepository,
    electricity_market_repository: ElectricityMarketRepository,
) -> None:
    child_missing_col = electricity_market_repository_extra_col.read_child_metering_points().df
    consumption_missing_col = electricity_market_repository_extra_col.read_consumption_metering_point_periods().df
    child = electricity_market_repository.read_child_metering_points().df
    consumption = electricity_market_repository.read_consumption_metering_point_periods().df
    assert child_missing_col.columns == child.columns
    assert consumption_missing_col.columns == consumption.columns


@pytest.mark.skip(reason="csv file does not contain data type")
def test__when_source_contains_wrong_data_type_raises_exception(
    electricity_market_repository_wrong_data_type: ElectricityMarketRepository,
) -> None:
    pass
