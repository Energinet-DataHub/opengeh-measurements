import inspect

from pyspark.sql import DataFrame

DEBUG = False


def debugging(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    caller = inspect.stack()[1].function
    print(f"######## In {caller}: {message}")  # noqa: T201

    df.show()
