import os
from dataclasses import dataclass

import pytest

import geh_calculated_measurements
from geh_calculated_measurements.testing.utilities.job_tester import JobTestFixture


@dataclass
class MockConfig:
    workspace_url: str
    databricks_token: str
    shared_keyvault_url: str


def test_wait_for_log_query_completion(monkeypatch):
    monkeypatch.setattr(
        geh_calculated_measurements.testing.utilities.job_tester, "WorkspaceClient", lambda *args, **kwargs: None
    )
    keyvault_url = os.getenv("AZURE_KEYVAULT_URL")
    assert keyvault_url is not None, "The Azure Key Vault name cannot be empty."
    config = MockConfig(shared_keyvault_url=keyvault_url, workspace_url="", databricks_token="")
    fixture = JobTestFixture(config, "integration_test_job")
    query = """
        AppTraces
        | where Properties.Subsystem == "test-subsystem"
        | where Properties["orchestration_instance_id"] == "does-not-exist"
    """
    with pytest.raises(ValueError, match="Failed to execute query:"):
        fixture.wait_for_log_query_completion(query)
