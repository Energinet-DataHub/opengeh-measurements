from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class SilverRepository(ABC):
    @abstractmethod
    def read_stream(self, table_name) -> DataFrame:
        """Defines a Streaming DataFrame on the silver table, from the given table name."""
        pass
