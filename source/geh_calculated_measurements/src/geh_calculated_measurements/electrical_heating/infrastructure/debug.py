import inspect
import logging

from pyspark.sql import DataFrame

DEBUG = True


def debugging(df: DataFrame, message: str | None = None) -> None:
    if not DEBUG:
        return

    if message is not None:
        logging.debug(message)
    else:
        calling_function = inspect.stack()[1].function
        logging.debug(calling_function)

    df.show()
