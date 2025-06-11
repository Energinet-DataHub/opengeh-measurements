from datetime import datetime
from typing import Any, cast

import pytest
from geh_common.application import GridAreaCodes
from geh_common.data_products.electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 import (
    schema,
)
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames
from geh_calculated_measurements.missing_measurements_log.domain.calculation import (
    _get_metering_point_periods_from_grid_areas,
)


@pytest.mark.parametrize(("grid_area_codes", "expected_count"), [(["300", "301"], 2), (["300"], 1), ([], 2), (None, 2)])
def test_get_metering_point_periods_from_specified_grid_area_codes(
    spark: SparkSession, grid_area_codes: GridAreaCodes | None, expected_count: int
):
    # Arrange
    data = [
        {
            ContractColumnNames.metering_point_id: "123456789",
            ContractColumnNames.grid_area_code: "300",
            ContractColumnNames.resolution: "PT1H",
            ContractColumnNames.period_from_date: datetime(2025, 3, 30, 0, 0),
            ContractColumnNames.period_to_date: None,
        },
        {
            ContractColumnNames.metering_point_id: "987654321",
            ContractColumnNames.grid_area_code: "301",
            ContractColumnNames.resolution: "PT15M",
            ContractColumnNames.period_from_date: datetime(2025, 3, 30, 0, 0),
            ContractColumnNames.period_to_date: None,
        },
    ]

    metering_point_periods = spark.createDataFrame(cast(list[Any], data), schema=schema)

    # Act
    actual = _get_metering_point_periods_from_grid_areas(metering_point_periods, grid_area_codes)

    # Assert
    assert actual.count() == expected_count
