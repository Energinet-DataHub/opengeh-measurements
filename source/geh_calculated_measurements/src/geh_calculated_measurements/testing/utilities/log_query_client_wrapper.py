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
        poll_interval_seconds: int = 5,
        timespan_minutes: int = 15,
    ) -> LogsQueryResult:
        """Execute an Azure Log Analytics query and wait for the result.

        Only returns when the query is successful and returns at least one row.
        """
        start_time = time.time()
        elapsed_time = 0

        while elapsed_time < timespan_minutes * 60:
            try:
                result = self.logs_query_client.query_workspace(
                    workspace_id, query, timespan=timedelta(timespan_minutes)
                )
                if result.status == LogsQueryStatus.SUCCESS and len(result.tables[0].rows) > 0:
                    return result
            except Exception:
                pass
            elapsed_time = time.time() - start_time
            print(f"Query did not complete in {elapsed_time} seconds. Retrying in {poll_interval_seconds} seconds...")  # noqa: T201
            time.sleep(poll_interval_seconds)

        raise TimeoutError(f"Query did not complete within {timespan_minutes * 60} seconds:\n{query}")
