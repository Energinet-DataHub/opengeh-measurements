from abc import ABC

import pyspark.sql.types as t
from geh_common.testing.dataframes import assert_schema
from pyspark.sql import DataFrame, SparkSession

nullable = True


class Table(ABC):
    fqn: str
    schema: t.StructType
    spark: SparkSession
    columns: list[str]
    nullable = True

    def __init__(self) -> None:
        if not hasattr(self, "fqn"):
            raise AttributeError("Table must define a fully qualified name `fqn`.")
        if not hasattr(self, "schema"):
            raise AttributeError("Table must define a schema.")
        if not hasattr(self, "spark"):
            raise AttributeError("Table must define a Spark session.")

    @classmethod
    def __init_subclass__(cls) -> None:
        """Doc."""
        super().__init_subclass__()

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

        def _read(self, *args, **kwargs):
            _df = self._read(*args, **kwargs)
            assert_schema(cls.schema, _df.schem)
            return _df

        cls.read = _read

    def read(self) -> DataFrame:
        return self.spark.table(self.fqn)


# Example implementation
class MyTable(Table):
    fqn = "my_database.my_table"

    metering_point_id = t.StructField("metering_point_id", t.StringType(), not Table.nullable)
    metering_point_type = t.StructField("metering_point_type", t.StringType(), not Table.nullable)
    observation_time = t.StructField("observation_time", t.TimestampType(), not Table.nullable)
    quantity = t.StructField("quantity", t.DecimalType(18, 3), not Table.nullable)
