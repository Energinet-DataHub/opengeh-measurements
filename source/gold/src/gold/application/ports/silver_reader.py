from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class SilverReader(ABC):
    @abstractmethod
    def read(self) -> DataFrame:
        """
        Define a Streaming DataFrame on a Table.
        """
        pass
