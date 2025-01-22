from abc import ABC, abstractmethod
from typing import Callable

from pyspark.sql import DataFrame


class GoldWriter(ABC):
    @abstractmethod
    def start(self, records: DataFrame, batch_operation: Callable[["DataFrame", int], None]):
        """Start the Gold Delta table writer, using the provided batch operation."""

    @abstractmethod
    def write(self, records: DataFrame):
        """Write records to the Gold Delta table"""
        pass
