﻿import pytest


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
    # TODO AJW
    assert True
    # assert_entry_point_exists(entry_point_name, module)