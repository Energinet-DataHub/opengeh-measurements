import inspect

from pyspark.sql import DataFrame

DEBUG = False
ROWS = 50


def debugging(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    caller = inspect.stack()[1].function
    print(f">> {message or ''} in {caller}")  # noqa: T201

    df.show(truncate=False, n=ROWS)
