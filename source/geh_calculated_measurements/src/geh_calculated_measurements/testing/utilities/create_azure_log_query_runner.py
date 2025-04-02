import os
from typing import Callable

import pytest
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.monitor.query import LogsQueryStatus

from geh_calculated_measurements.testing.utilities.log_query_client_wrapper import LogQueryClientWrapper
from tests import SPARK_CATALOG_NAME


def create_azure_log_query_runner(monkeypatch: pytest.MonkeyPatch) -> Callable[[str, int], LogsQueryStatus]:
    """Create a function that can be used to run an Azure Log Analytics query and wait for the result.

    Args:
        monkeypatch (pytest.MonkeyPatch): The monkeypatch fixture.

    Returns:
        Callable[[str, int], LogsQueryStatus]: A function that can be used to run an Azure Log Analytics query and wait for the result.
            The function takes two arguments: the query to run and the timeout in minutes.
    """
    credentials = DefaultAzureCredential()
    azure_logs_query_client = LogQueryClientWrapper(credentials)

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

    def query_client(query: str, timeout_minutes: int = 15):
        """Run an Azure Log Analytics query and wait for the result.

        Args:
            query (str): The query to run.
            timeout_minutes (int, optional): The timeout in minutes. Defaults to 15.

        Returns:
            LogsQueryStatus: The status of the query.
        """
        return azure_logs_query_client.wait_for_condition(workspace_id, query.strip(), timespan_minutes=timeout_minutes)

    return query_client
