import pytest

from capacity_settlement import entry_point
from source.capacity_settlement.tests.capacity_settlement_tests.entry_point_test_util import (
    assert_entry_point_exists,
)


# test


@pytest.mark.parametrize(
    "entry_point_name",
    [
        "execute",
    ],
)
def test__entry_point_exists(
    installed_package: None,
    entry_point_name: str,
) -> None:
    assert_entry_point_exists(entry_point_name, entry_point)
