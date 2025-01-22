from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame


class GoldRepository(ABC):
    @abstractmethod
    def start_write_stream(self, records: DataFrame, query_name: str, table_name: str, batch_operation: Callable[["DataFrame", int], None]) -> None:
        """Starts a streaming query to write records to the Gold database."""

    @abstractmethod
    def append(self, records: DataFrame, table_name: str) -> None:
        """Writes records to the Gold database, using the append method on the given table."""
        pass
