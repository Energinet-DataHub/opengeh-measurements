import pytest

from source.electrical_heating.src.electrical_heating import entry_point as module
from source.tests.test_common.entry_points.entry_point_test_util import (
    assert_entry_point_exists,
)
from source.electrical_heating.src.electrical_heating.domain.chba_test import main


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
    assert_entry_point_exists(entry_point_name, module)


def test_main() -> None:
    res = main()
    assert res is not None
    assert res == 'Hello World'


def test_main_scenario_failz() -> None:
    res = main()
    assert res is not None
    assert res == 'Hello World'

def test_spark_session(spark):  # Use the 'spark' fixture as an argument
    assert spark is not None  # Check that the Spark session was successfully created
    # Your test logic involving the Spark session
    df = spark.createDataFrame([(1, "foo"), (2, "bar")], ["id", "value"])
    assert df.count() == 2
    df_as_list = df.collect()
    assert df_as_list[0]["value"] == "foo"
