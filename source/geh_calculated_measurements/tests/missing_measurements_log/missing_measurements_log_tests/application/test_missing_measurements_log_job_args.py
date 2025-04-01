import os
import sys
import uuid
from datetime import datetime, timezone

from geh_calculated_measurements.missing_measurements_log.application.missing_measurements_log_args import (
    MissingMeasurementsLogArgs,
)
from tests import PROJECT_ROOT, SPARK_CATALOG_NAME, create_job_environment_variables
from tests.missing_measurements_log import CONTAINER_NAME

_CONTRACTS_PATH = (PROJECT_ROOT / "src" / "geh_calculated_measurements" / CONTAINER_NAME / "contracts").as_posix()

_EXPECTED_ORCHESTRATION_INSTANCE_ID = uuid.UUID("12345678-9fc8-409a-a169-fbd49479d711")
_EXPECTED_TIME_ZONE = "Europe/Copenhagen"
_EXPECTED_CATALOG_NAME = SPARK_CATALOG_NAME
_EXPECTED_PERIOD_START_DATETIME = datetime(2022, 5, 31, 22, 0, 0, tzinfo=timezone.utc)
_EXPECTED_PERIOD_END_DATETIME = datetime(2022, 6, 1, 22, 0, 0, tzinfo=timezone.utc)
_EXPECTED_GRID_AREA_CODES = ["804", "805"]


def _get_contract_parameters(filename: str) -> list[str]:
    with open(filename) as file:
        text = file.read()
        text = text.replace("{orchestration-instance-id}", str(_EXPECTED_ORCHESTRATION_INSTANCE_ID))
        lines = text.splitlines()
        return list(filter(lambda line: not line.startswith("#") and len(line) > 0, lines))


def test_when_parameters__parses_parameters_from_contract(monkeypatch) -> None:
    """
    This test ensures that the job accepts
    the arguments that are provided by the client.
    """

    # Arrange
    sys_args = ["dummy_script_name"] + _get_contract_parameters(f"{_CONTRACTS_PATH}/parameters-reference.txt")
    monkeypatch.setattr(sys, "argv", sys_args)
    monkeypatch.setattr(os, "environ", create_job_environment_variables())
    actual_args = MissingMeasurementsLogArgs()

    # Assert
    assert actual_args.orchestration_instance_id == _EXPECTED_ORCHESTRATION_INSTANCE_ID
    assert actual_args.time_zone == _EXPECTED_TIME_ZONE
    assert actual_args.catalog_name == _EXPECTED_CATALOG_NAME
    assert actual_args.grid_area_codes == _EXPECTED_GRID_AREA_CODES
    assert actual_args.period_start_datetime == _EXPECTED_PERIOD_START_DATETIME
    assert actual_args.period_end_datetime == _EXPECTED_PERIOD_END_DATETIME
