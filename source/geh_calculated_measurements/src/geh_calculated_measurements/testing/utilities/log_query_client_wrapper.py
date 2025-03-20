import time
from datetime import timedelta

from azure.identity import DefaultAzureCredential
from azure.monitor.query import LogsQueryClient, LogsQueryResult, LogsQueryStatus


class LogQueryClientWrapper:
    def __init__(self, credentials: DefaultAzureCredential) -> None:
        self.logs_query_client = LogsQueryClient(credentials)

    def wait_for_condition(
        self,
        workspace_id: str,
        query: str,
        timeout_seconds: int = 300,
        poll_interval_seconds: int = 5,
        timespan_minutes: int = 15,
    ) -> LogsQueryResult:
        """Wait for a condition to be met by polling a query on an Azure Log Analytics workspace. Only returns when the query is successful and returns at least one row."""
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                result = self.logs_query_client.query_workspace(
                    workspace_id, query, timespan=timedelta(timespan_minutes)
                )
                if result.status == LogsQueryStatus.SUCCESS and len(result.tables[0].rows) > 0:
                    return result
            except Exception:
                pass

            time.sleep(poll_interval_seconds)

        raise TimeoutError(f"Query did not complete within {timeout_seconds} seconds. Query: {query}.")
