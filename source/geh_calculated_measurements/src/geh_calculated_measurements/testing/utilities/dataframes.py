# TODO BJM: This will be moved to geh_common and integrated with testsession.local.settings.yml in other PRs
import inspect
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame

TESTING = False
ROWS = 50


def testing(name: str | None = None) -> Callable[..., Any]:
    """Use this decorator to log data frame return value of functions."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, DataFrame):
                _log_dataframe(result, f"Result of function '{func.__name__}'")
            return result

        return wrapper

    return decorator


def _log_dataframe(df: DataFrame, message: str | None = None) -> None:
    if not TESTING:
        return

    caller = inspect.stack()[2]
    file_name = Path(caller.filename).name
    print(f">>> In function '{caller.function}' ({file_name}:{caller.lineno}): {message or ''}")  # noqa: T201

    df.show(truncate=False, n=ROWS)
