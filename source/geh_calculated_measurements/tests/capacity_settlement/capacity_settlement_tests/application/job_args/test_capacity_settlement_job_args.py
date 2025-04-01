import os
import sys
import uuid

from geh_calculated_measurements.capacity_settlement.application.capacity_settlement_args import (
    CapacitySettlementArgs,
)
from tests import PROJECT_ROOT, create_job_environment_variables

_CONTRACTS_PATH = f"{PROJECT_ROOT}/src/geh_calculated_measurements/capacity_settlement/contracts/"

DEFAULT_ORCHESTRATION_INSTANCE_ID = uuid.UUID("12345678-9fc8-409a-a169-fbd49479d711")
DEFAULT_CALCULATION_MONTH = 1
DEFAULT_CALCULATION_YEAR = 2021


def _get_contract_parameters(filename: str) -> list[str]:
    with open(filename) as file:
        text = file.read()
        text = text.replace("{orchestration-instance-id}", str(DEFAULT_ORCHESTRATION_INSTANCE_ID))
        text = text.replace("{calculation-month}", str(DEFAULT_CALCULATION_MONTH))
        text = text.replace("{calculation-year}", str(DEFAULT_CALCULATION_YEAR))
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
    actual_args = CapacitySettlementArgs()

    # Assert
    assert actual_args.orchestration_instance_id == DEFAULT_ORCHESTRATION_INSTANCE_ID
    assert actual_args.calculation_month == DEFAULT_CALCULATION_MONTH
    assert actual_args.calculation_year == DEFAULT_CALCULATION_YEAR
