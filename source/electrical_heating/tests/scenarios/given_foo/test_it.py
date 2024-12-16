from typing import Any, Tuple

import pytest
from pyspark.sql import DataFrame

from scenarios.test_common import assert_output, get_output_names


# get_test_case_names() -> ["output_table_1", "output_table_2"]
# test_cases: All actual and expected dataframes
@pytest.mark.parametrize("test_case_name", get_test_case_names())
def test__equals_expected(
    test_case_name: str,
    test_cases: list[Tuple[str, DataFrame, DataFrame]],
) -> None:
    test_case = next(tc for tc in test_cases if tc[0] == test_case_name)
    assert_data_frames(
        test_case[1], test_case[2]
    )


@pytest.mark.parametrize("test_case_name", get_test_case_names())
def test__equals_expected(
    test_case_name: str,
    test_cases: TestCases,
) -> None:
    test_case = test_cases[test_case_name]
    assert_data_frames(
        test_case.actual, test_case.expected
    )
