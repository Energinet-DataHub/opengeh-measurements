from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame


class GoldWriter(ABC):
    @abstractmethod
    def start_stream(self, records: DataFrame, batch_operation: Callable[["DataFrame", int], None]):
        """Starts a streaming query to process data in micro-batches, given a read stream and a batch operation."""

    @abstractmethod
    def append(self, records: DataFrame):
        """Write records to the Gold Delta table, using the append method."""
        pass
