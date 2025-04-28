import random
from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp

day_month_year_date_time_formatting = "%d-%m-%YT%H:%M:%S%z"


def get_datetime(
    year: int = 1900, month: int = 1, day: int = 1, hour: int = 1, minute: int = 1, second: int = 1
) -> datetime:
    """
    date string has to be in format 'dd-mm-YYYYTHH:MM:SS+zzzz'

    date string example: '01-01-2021T00:00:00+0000'
    """

    datetime_str = f"{day}-{month}-{year}T{hour}:{minute}:{second}+0000"

    return datetime.strptime(datetime_str, day_month_year_date_time_formatting)


def get_proto_timestamp(
    year: int = 1900, month: int = 1, day: int = 1, hour: int = 1, minute: int = 1, second: int = 1
) -> Timestamp:
    """
    Converts a datetime to a protobuf Timestamp.
    """
    dt = get_datetime(year, month, day, hour, minute, second)
    timestamp = Timestamp()
    timestamp.FromDatetime(dt)
    return timestamp


def random_datetime() -> datetime:
    year = random.randint(1900, 2021)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return get_datetime(year, month, day, hour, minute, second)
