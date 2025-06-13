import time

from databricks.sdk.service.sql import StatementResponse
from geh_common.databricks import DatabricksApiClient

from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings


class DatabricksAssertionHelper:
    def __init__(self) -> None:
        self.databricks_settings = DatabricksSettings()  # type: ignore
        self.databricks_api_client = DatabricksApiClient(
            self.databricks_settings.token,
            self.databricks_settings.workspace_url,
        )

    def assert_row_persisted(self, query, timeout: int = 60, poll_interval: int = 5) -> None:
        start_time = time.time()
        print(f"Executing query: {query}")  # noqa: T201

        while (time.time() - start_time) < timeout:
            print(f"{time.time() - start_time:.2f} seconds elapsed, waiting for query: {query}")  # noqa: T201
            print(f"Polling for query: {query}")  # noqa: T201
            result = self.databricks_api_client.execute_statement(
                warehouse_id=self.databricks_settings.warehouse_id, statement=query
            )

            count = self._extract_count_from_result(result)
            print(f"Query result count: {count}")  # noqa: T201

            if count > 0:
                return

            time.sleep(poll_interval)

        print(f"Timeout reached after {timeout} seconds for query: {query}")  # noqa: T201
        raise AssertionError(f"No row found for query: {query}")

    def _extract_count_from_result(self, result: StatementResponse) -> int:
        if not result.result or not result.result.data_array:
            return 0
        count_row = result.result.data_array[0]
        count_value = count_row[0]
        return int(count_value)
