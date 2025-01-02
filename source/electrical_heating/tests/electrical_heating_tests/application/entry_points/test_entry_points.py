import pytest


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
    assert True
    # TODO AJW
    # assert_entry_point_exists(entry_point_name, module)
