from datetime import datetime

import pytest
from geh_common.application import GridAreaCodes
from geh_common.data_products.electricity_market_measurements_input.missing_measurements_log_metering_point_periods_v1 import (
    schema,
)
from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.domain.calculation import (
    _get_metering_point_periods_from_grid_areas,
)


@pytest.mark.parametrize(("grid_area_codes", "expected_count"), [(["300", "301"], 2), (["300"], 1), ([], 2), (None, 2)])
def test_get_metering_point_periods_from_specified_grid_area_codes(
    spark: SparkSession, grid_area_codes: GridAreaCodes | None, expected_count: int
):
    # Arrange
    data = [
        ("123456789", "300", "PT1H", datetime(2025, 3, 30, 0, 0), None),
        ("987654321", "301", "PT15M", datetime(2025, 3, 30, 0, 0), None),
    ]

    metering_point_periods = spark.createDataFrame(data, schema=schema)

    # Act
    actual = _get_metering_point_periods_from_grid_areas(metering_point_periods, grid_area_codes)

    # Assert
    assert actual.count() == expected_count
