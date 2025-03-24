from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class Protobuf(ABC):
    @property
    @abstractmethod
    def version(self) -> str:
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

    @abstractmethod
    def unpack(self) -> tuple[DataFrame, DataFrame]:
        pass
