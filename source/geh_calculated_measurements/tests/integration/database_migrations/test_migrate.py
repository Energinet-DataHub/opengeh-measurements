from geh_calculated_measurements.database_migrations.entry_point import migrate
from geh_calculated_measurements.testing.utilities.create_azure_log_query_runner import (
    LogsQueryStatus,
    create_azure_log_query_runner,
)
from tests import ensure_calculated_measurements_databases_exist


def test__when_running_migrate__then_log_is_produced(spark, monkeypatch):
    # Arrange
    azure_query_runnner = create_azure_log_query_runner(monkeypatch)
    timeout_minutes = 15
    ensure_calculated_measurements_databases_exist(spark)

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
