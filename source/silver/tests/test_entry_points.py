import pytest

from source.silver.src.silver import entry_points as module
from source.tests.test_common.entry_points.entry_point_test_util import (
    assert_entry_point_exists,
)


@pytest.mark.parametrize(
    "entry_point_name",
    [
        "execute_silver_stream",
        "migrate",
    ],
)
def test__entry_point_exists(
    entry_point_name: str,
) -> None:
    assert_entry_point_exists(entry_point_name, module)
