import inspect
from pathlib import Path

from pyspark.sql import DataFrame

DEBUG = False
ROWS = 50


def debugging(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    caller = inspect.stack()[1]
    file_name = Path(caller.filename).name
    print(f">> {caller.function}({file_name}:{caller.lineno}): {message or ''}")  # noqa: T201

    df.show(truncate=False, n=ROWS)
