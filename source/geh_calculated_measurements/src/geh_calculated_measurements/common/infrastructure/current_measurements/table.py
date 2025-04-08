from abc import ABC

import pyspark.sql.types as t
from geh_common.testing.dataframes import assert_schema
from pyspark.sql import DataFrame, SparkSession


class Table(ABC):
    fully_qualified_name: str
    schema: t.StructType
    spark: SparkSession
    columns: list[str]

    def __init__(self) -> None:
        if not hasattr(self, "fully_qualified_name"):
            raise AttributeError("Table must define a fully qualified name.")
        if not hasattr(self, "schema"):
            raise AttributeError("Table must define a schema.")
        if not hasattr(self, "spark"):
            raise AttributeError("Table must define a Spark session.")

    @classmethod
    def __init_subclass__(cls) -> None:
        """Special method that is called automatically when a class is subclassed."""
        schema = []
        columns = []

        d = {**cls.__class__.__dict__, **cls.__dict__}
        for name, field in d.items():
            if isinstance(field, t.StructField):
                schema.append(field)
                columns.append(field.name)
                setattr(cls, name, field.name)
                setattr(cls, f"{name}_type", field.dataType)

        cls.schema = t.StructType(schema)
        cls.columns = columns
        cls.spark = SparkSession.builder.getOrCreate()
        cls._read = cls.read

        def _read(self, *args, **kwargs) -> DataFrame:
            """Read the table from the Spark session and assert the schema."""
            _df = self._read(*args, **kwargs)
            assert_schema(cls.schema, _df.schem)
            return _df

        cls.read = _read

    def read(self) -> DataFrame:
        return self.spark.table(self.fully_qualified_name)
