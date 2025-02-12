import pytest
from environment_configuration import EnvironmentConfiguration
from sql_script import sql_script
from testcommon.container_test import DatabricksApiClient

from opengeh_electrical_heating.infrastructure.measurements.measurements_gold.database_definitions import (
    MeasurementsGoldDatabase,
)


@pytest.fixture(scope="session")
def environment_configuration() -> EnvironmentConfiguration:
    return EnvironmentConfiguration()


class DatabricksApiClientWithSeeding(DatabricksApiClient):
    def seed(
        self,
        warehouse_id: str,
        catalog: str,
        schema: str,
        statement: str,
    ) -> "DatabricksApiClientWithSeeding":
        """Execute a SQL statement for data seeding.

        Args:
            warehouse_id (str): Databricks warehouse/cluster ID
            catalog (str): Databricks catalog name
            schema (str): Database schema name
            statement (str): SQL statement to execute

        Returns:
            DatabricksApiClientWithSeeding: Self reference for method chaining
        """
        try:
            response = self.client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                catalog=catalog,
                schema=schema,
                statement=statement,
            )

            if response.status.state == "FAILED":
                raise Exception(f"Seeding failed: {response.status.error}")

            return self

        except Exception as e:
            raise Exception(f"Failed to execute seeding statement: {str(e)}")


@pytest.fixture(scope="session")
def databricks_api_client(environment_configuration: EnvironmentConfiguration) -> DatabricksApiClientWithSeeding:
    databricksApiClient = DatabricksApiClientWithSeeding(
        environment_configuration.databricks_token,
        environment_configuration.workspace_url,
    ).seed(
        warehouse_id=environment_configuration.cluster_id,
        catalog=environment_configuration.catalog_name,
        schema=MeasurementsGoldDatabase.DATABASE_NAME,
        statement=sql_script,
    )
    return databricksApiClient
