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

        self.spark = SparkSession.builder.getOrCreate()

    @classmethod
    def __init_subclass__(cls) -> None:
        """Automatically called when a class is subclassed."""
        """The parameter cls refers to the subclass of the Table class that is being created"""
        schema = []
        columns = []

        # The following ensures that both the subclass's attributes and
        # any inherited or metaclass-level attributes are included when iterating over dic.
        d = {**cls.__class__.__dict__, **cls.__dict__}
        for name, field in d.items():
            if isinstance(field, t.StructField):
                schema.append(field)
                columns.append(field.name)
                setattr(cls, name, field.name)
                setattr(cls, f"{name}_type", field.dataType)

        cls.schema = t.StructType(schema)
        cls.columns = columns
        cls._read = cls.read

        def _read(self, *args, **kwargs) -> DataFrame:
            _df = self._read(*args, **kwargs)
            assert_schema(cls.schema, _df.schem)
            return _df

        cls.read = _read

    def read(self) -> DataFrame:
        return self.spark.table(self.fully_qualified_name)
