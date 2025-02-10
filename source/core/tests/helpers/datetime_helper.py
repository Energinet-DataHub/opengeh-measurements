from datetime import datetime

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
