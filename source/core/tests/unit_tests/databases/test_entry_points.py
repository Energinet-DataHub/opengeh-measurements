import sys

import pytest
from pytest_mock import MockFixture

import core.databases.entry_points as sut


def test__optimize_table__with_args__executes_optimize_on_table(
    monkeypatch: pytest.MonkeyPatch, mocker: MockFixture
) -> None:
    # Arrange
    database = "test_db"
    table = "test_table"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dummy_script_name",
            f"--database={database}",
            f"--table={table}",
        ],
    )

    mocked_optimization = mocker.Mock()
    mocker.patch.object(sut, "optimization", mocked_optimization)

    # Act
    sut.optimize_table()

    # Assert
    mocked_optimization.optimize_table.assert_called_once_with(
        database,
        table,
    )
