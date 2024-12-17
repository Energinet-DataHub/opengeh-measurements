from typing import Tuple

import pytest
from pyspark.sql import DataFrame


# get_test_case_names() -> ["output_table_1", "output_table_2"]
# test_cases: All actual and expected dataframes
@pytest.mark.parametrize("test_case_name", get_then_names())
def test__equals_expected(
    then_name: str,
    test_cases: list[Tuple[str, DataFrame, DataFrame]],
) -> None:
    test_case = next(tc for tc in test_cases if tc[0] == then_name)
    assert_data_frames(
        test_case[1], test_case[2]
    )


@pytest.mark.parametrize("test_case_name", get_then_names())
def test__equals_expected(
    then_name: str,
    test_cases: TestCases,
) -> None:
    test_case = test_cases[then_name]
    assert_data_frames(
        test_case.actual, test_case.expected
    )
