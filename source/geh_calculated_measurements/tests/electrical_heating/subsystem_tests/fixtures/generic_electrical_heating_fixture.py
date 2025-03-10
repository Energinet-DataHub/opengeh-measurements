import uuid

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryPartialResult, LogsQueryResult
from databricks.sdk.service.jobs import RunResultState
from geh_common.databricks.databricks_api_client import DatabricksApiClient

from geh_calculated_measurements.electrical_heating.infrastructure import MeasurementsGoldDatabaseDefinition
from geh_calculated_measurements.testing import LogQueryClientWrapper
from tests.electrical_heating.subsystem_tests.environment_configuration import EnvironmentConfiguration


class CalculationInput:
    orchestration_instance_id: uuid.UUID
    job_id: int


class JobState:
    run_id: int
    run_result_state: RunResultState
    calculation_input: CalculationInput = CalculationInput()


def seed_data_query(catalog: str, schema: str, table: str = "measurements") -> str:
    return f"""INSERT INTO {catalog}.{schema}.{table} (
    transaction_id, quantity, transaction_creation_datetime, created, modified, -- dynamic variables
  metering_point_id, observation_time, quality, metering_point_type, orchestration_type -- static variables
)
SELECT
    REPLACE(CAST(uuid() AS VARCHAR(50)), '-', '') AS transaction_id, 
    CAST(RAND() * 1000000 AS DECIMAL(18, 3)) AS quantity, 
    GETDATE() AS transaction_creation_datetime, 
    GETDATE() AS created, -- created
    GETDATE() AS modified, -- modified
    '170000030000000201' AS metering_point_id, 
    '2024-11-30T23:00:00Z' AS observation_time, 
    'measured' AS quality, -- quality
    'consumption' AS metering_point_type,
    'submitted' AS orchestration_type"""


class BaseJobFixture:
    def __init__(self, environment_configuration: EnvironmentConfiguration, job_name: str, seed_data: bool):
        self.databricks_api_client = DatabricksApiClient(
            environment_configuration.databricks_token,
            environment_configuration.workspace_url,
        )
        if seed_data:
            self.databricks_api_client.execute_statement(
                warehouse_id=environment_configuration.warehouse_id,
                statement=seed_data_query(
                    catalog=environment_configuration.catalog_name,
                    schema=MeasurementsGoldDatabaseDefinition.DATABASE_NAME,
                ),
            )
        self.job_state = JobState()
        self.credentials = DefaultAzureCredential()
        self.azure_logs_query_client = LogQueryClientWrapper(self.credentials)
        self.secret_client = SecretClient(
            vault_url=f"https://{environment_configuration.shared_keyvault_name}.vault.azure.net/",
            credential=self.credentials,
        )
        self.job_name = job_name

    def get_job_id(self) -> int:
        return self.databricks_api_client.get_job_id(self.job_name)

    def start_job(self, calculation_input: CalculationInput) -> int:
        params = [
            f"--orchestration-instance-id={str(calculation_input.orchestration_instance_id)}",
        ]
        return self.databricks_api_client.start_job(calculation_input.job_id, params)

    def wait_for_job_to_completion(self, run_id: int) -> RunResultState:
        return self.databricks_api_client.wait_for_job_completion(run_id)

    def wait_for_log_query_completion(self, query: str) -> LogsQueryResult | LogsQueryPartialResult:
        workspace_id = self.secret_client.get_secret("log-shared-workspace-id").value
        if workspace_id is None:
            raise ValueError("The Azure log analytics workspace ID cannot be empty.")

        return self.azure_logs_query_client.wait_for_condition(workspace_id, query)
