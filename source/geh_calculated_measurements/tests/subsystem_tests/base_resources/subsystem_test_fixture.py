import uuid
from datetime import datetime, timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks.databricks_api_client import DatabricksApiClient
from geh_common.domain.types import MeteringPointType

from geh_calculated_measurements.testing import LogQueryClientWrapper
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder


class Input:
    orchestration_instance_id: uuid.UUID
    job_id: int
    job_parameters: dict


class JobState:
    run_id: int
    run_result_state: RunResultState

    def __init__(self) -> None:
        self.input = Input()


METERING_POINT_ID = "170000040000000201"
CALCULATION_YEAR = 2025
CALCULATION_MONTH = 1
FIRST_OBSERVATION_TIME = datetime(2025, 1, 1, 23, 0, 0)


class SubsystemTestFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration) -> None:
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )

        self.job_state = JobState()
        self.credentials = DefaultAzureCredential()
        self.environment_configuration = environment_configuration
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )

    def get_job_id(self, job_name: str) -> int:
        return self.databricks_api_client.get_job_id(job_name)

    def start_job(self, input: Input) -> int:
        params_list = []
        for key, value in input.job_parameters.items():
            params_list.append(f"--{key}={value}")

        return self.databricks_api_client.start_job(input.job_id, params_list)

    def wait_for_job_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(
        self, query: str, job_state: JobState
    ) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        if job_state.run_result_state != RunResultState.SUCCESS:
            raise Exception("A previous test did not complete successfully.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)

    def seed_gold_table(self, rows: list[GoldTableRow]) -> None:
        table_seeder = GoldTableSeeder(self.environment_configuration)
        table_seeder.seed(rows)

    def get_gold_table_rows(self) -> list[GoldTableRow]:
        return [
            GoldTableRow(
                metering_point_id=METERING_POINT_ID,
                observation_time=FIRST_OBSERVATION_TIME + timedelta(hours=i),
                metering_point_type=MeteringPointType.CONSUMPTION,
                quantity=i,
            )
            for i in range(10)
        ]
