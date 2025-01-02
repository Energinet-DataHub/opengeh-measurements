import pytest

import electrical_heating.main as entry_point_module
from test_common.entry_points.entry_point_test_util import assert_entry_point_exists


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
    assert_entry_point_exists(entry_point_name, entry_point_module)
