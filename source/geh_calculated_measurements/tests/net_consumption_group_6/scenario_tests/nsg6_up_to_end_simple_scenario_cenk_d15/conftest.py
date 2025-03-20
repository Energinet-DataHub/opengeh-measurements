import pytest
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T


@pytest.fixture(autouse=True, scope="module")
def patch() -> None:
    patch = pytest.MonkeyPatch()

    def patched_current_timestamp() -> Column:
        return F.lit("2024-12-31T23:00:00Z").cast(T.TimestampType())

    patch.setattr(
        F,
        "current_timestamp",
        patched_current_timestamp,
    )
