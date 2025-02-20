import inspect
import logging

from pyspark.sql import DataFrame

DEBUG = True


def debugging(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    message = message or inspect.stack()[1].function
    logging.debug(message)

    df.show()
