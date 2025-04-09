import os

import pytest
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryStatus
from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from geh_calculated_measurements.database_migrations import migrations_runner
from geh_calculated_measurements.database_migrations.entry_point import migrate
from geh_calculated_measurements.testing.utilities.create_azure_log_query_runner import (
    create_azure_log_query_runner,
)
from tests import SPARK_CATALOG_NAME
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


def test__when_running_migrations__then_calculated_measurements_v1__is_streamable(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
):
    # Arrange
    credentials = DefaultAzureCredential()

    azure_keyvault_url = os.getenv("AZURE_KEYVAULT_URL")
    assert azure_keyvault_url is not None, f"The Azure Key Vault URL cannot be empty: {azure_keyvault_url}"
    secret_client = SecretClient(vault_url=azure_keyvault_url, credential=credentials)
    workspace_id = secret_client.get_secret("AZURE-LOGANALYTICS-WORKSPACE-ID").value
    if workspace_id is None:
        raise ValueError("The Azure log analytics workspace ID cannot be empty.")
    application_insights_connection_string = secret_client.get_secret("AZURE-APPINSIGHTS-CONNECTIONSTRING").value
    if application_insights_connection_string is None:
        raise ValueError("The Azure Application Insights connection string cannot be empty.")

    monkeypatch.setenv("CATALOG_NAME", SPARK_CATALOG_NAME)
    monkeypatch.setenv("APPLICATIONINSIGHTS_CONNECTION_STRING", application_insights_connection_string)

    # Act
    migrate()

    # Assert
    catalog_name = EnvironmentConfiguration().catalog_name
    database_name = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    view_name = CalculatedMeasurementsDatabaseDefinition.CALCULATED_MEASUREMENTS_VIEW_NAME
    # Set the current catalog to Unity Catalog
    # spark.catalog.setCurrentCatalog("ctl_shres_d_we_001")
    # spark.sql("USE CATALOG ctl_shres_d_we_001") # [PARSE_SYNTAX_ERROR] Syntax error at or near 'ctl_shres_d_we_001': extra input 'ctl_shres_d_we_001'.(line 1, pos 12)

    print("################")
    # print(spark.catalog.currentCatalog())
    # for catalog in spark.catalog.listCatalogs():
    #     print("Catalog: " + catalog.name)
    #     schemas = spark.catalog.listDatabases()
    #     for schema in schemas:
    #         print("    " + schema.name)

    # Read the view as a streaming source using readStream
    streaming_df = spark.readStream.format("delta").table(f"{database_name}.{view_name}")
    query = streaming_df.writeStream.format("memory").queryName("test_query").start()

    try:
        # Wait for the streaming query to initialize
        query.awaitTermination(timeout=5)
        # Assert that the query is active and using the view as a source
        assert query.isActive, "The streaming query is not active"
    finally:
        # Stop the streaming query to clean up resources
        query.stop()


def test__when_running_migrate__then_log_is_produced(spark: SparkSession, monkeypatch: pytest.MonkeyPatch):
    # Arrange
    azure_query_runnner = create_azure_log_query_runner(monkeypatch)
    timeout_minutes = 15
    monkeypatch.setattr(
        migrations_runner, "_migrate", lambda name, subs: None
    )  # Mock this function to avoid actual migration

    # Act
    expected_log_messages = [
        "Initializing migrations with:\\nLogging Settings:",
        "Initializing migrations with:\\nCatalog Settings:",
    ]
    migrate()

    # Assert
    for message in expected_log_messages:
        query = f"""
            AppTraces
            | where Properties.Subsystem == "measurements"
            | where AppRoleName == "dbr-calculated-measurements"
            | where Message startswith_cs "{message}"
            | where TimeGenerated > ago({timeout_minutes}m)
        """

        query_result = azure_query_runnner(query, timeout_minutes=timeout_minutes)
        assert query_result.status == LogsQueryStatus.SUCCESS, f"The query did not complete successfully:\n{query}"
        assert query_result.tables[0].rows, (
            f"No logs were found for the given query:\n{query}\n---\n{query_result.tables}"
        )
