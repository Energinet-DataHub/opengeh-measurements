import random
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent.parent
TESTS_ROOT = PROJECT_ROOT / "tests"

SPARK_CATALOG_NAME = "spark_catalog"
MEASUREMENTS_GOLD_TABLE_NAME = "measurements"
TIME_ZONE = "Europe/Copenhagen"


@dataclass
class DataProduct:
    database_name: str
    view_name: str
    schema: Any


class CalculationType(IntEnum):
    """
    Calculation types for the measurements.
    The numbers are according to https://energinet.atlassian.net/wiki/spaces/D3/pages/1474035715/Reserved+Test+Data
    """

    ELECTRICAL_HEATING = 3
    CAPACITY_SETTLEMENT = 4
    NET_CONSUMPTION = 5
    MISSING_MEASUREMENTS_LOG = 6
    CORE = 9


# TODO BJM: Move to geh_common
def create_random_metering_point_id(calculation_type: CalculationType):
    position = 8
    id = "".join(random.choice("0123456789") for _ in range(18))
    return id[:position] + str(calculation_type) + id[position + 1 :]


def create_job_environment_variables(eletricity_market_path: str = "some_path") -> dict:
    return {
        "CATALOG_NAME": SPARK_CATALOG_NAME,
        "TIME_ZONE": TIME_ZONE,
        "ELECTRICITY_MARKET_DATA_PATH": eletricity_market_path,
        "APPLICATIONINSIGHTS_CONNECTION_STRING": "some_connection_string",
    }
