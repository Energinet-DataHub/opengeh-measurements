from typing import Tuple

import pytest
from pyspark.sql import DataFrame

from testcommon.etl import assert_dataframes, get_then_names


@pytest.mark.parametrize("test_case_name", get_then_names())
def test_get_then_names(test_case_name: str, test_cases: list[Tuple[str, DataFrame, DataFrame]]) -> None:
    test_case = next(test_case for test_case in test_cases if test_case[0] == test_case_name)
    assert_dataframes(test_case[1], test_cases[2])
