import inspect
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame

DEBUG = True
ROWS = 50


def log_dataframe(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    caller = inspect.stack()[1]
    file_name = Path(caller.filename).name
    print(f">> {caller.function}({file_name}:{caller.lineno}): {message or ''}")  # noqa: T201

    df.show(truncate=False, n=ROWS)


def debugging(name: str | None = None) -> Callable[..., Any]:
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, DataFrame):
                log_dataframe(result, f"Output of {func.__name__}")
            return result

        return wrapper

    return decorator
