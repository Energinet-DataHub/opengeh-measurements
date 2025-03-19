import uuid
from unittest.mock import patch

from pyspark.sql import SparkSession

from geh_calculated_measurements.missing_measurements_log.entry_point import execute
from tests import PROJECT_ROOT

_CONTRACTS_PATH = (
    PROJECT_ROOT / "src" / "geh_calculated_measurements" / "missing_measurements_log" / "contracts"
).as_posix()


def _get_contract_parameters(orchestration_instance_id: uuid.UUID) -> list[str]:
    file_name = f"{_CONTRACTS_PATH}/parameters-reference.txt"
    with open(file_name) as file:
        text = file.read()
        text = text.replace("{orchestration-instance-id}", str(orchestration_instance_id))
        lines = text.splitlines()
        return list(filter(lambda line: not line.startswith("#") and len(line) > 0, lines))


def _get_sys_argv_from_contract(orchestration_instance_id: uuid.UUID) -> list[str]:
    return ["dummy_script_name"] + _get_contract_parameters(orchestration_instance_id)


def _create_job_environment_variables() -> dict[str, str]:
    return {
        "CATALOG_NAME": "spark_catalog",
        "TIME_ZONE": "Europe/Copenhagen",
    }


def test_execute(
    spark: SparkSession,
) -> None:
    # Arrange
    orchestration_instance_id = uuid.uuid4()
    sys_argv = _get_sys_argv_from_contract(orchestration_instance_id)

    # Act
    with patch("sys.argv", sys_argv):
        with patch.dict("os.environ", _create_job_environment_variables()):
            execute()

    # Assert
    # TODO: Assert that data is written in delta. For now the test only asserts that no exception is raised.
