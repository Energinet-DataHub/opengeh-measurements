from abc import ABC

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


class Table(ABC):
    schema: T.StructType
    spark: SparkSession
    columns: list[str]

    def __init__(self):
        assert hasattr(self, "fqn"), "Table must have a fully qualified name"
        assert hasattr(self, "schema"), "Table must have a schema"
        assert hasattr(self, "spark"), "Table must have a spark session"

    def __init_subclass__(cls):
        """Doc."""
        schema = []
        columns = []

        d = {**cls.__class__.__dict__, **cls.__dict__}
        for name, field in d.items():
            if isinstance(field, T.StructField):
                schema.append(field)
                columns.append(field.name)
                setattr(cls, name, field.name)
                setattr(cls, f"{name}_type", field.dataType)

        cls.schema = T.StructType(schema)
        cls.columns = columns
        cls.spark = SparkSession.builder.getOrCreate()
        cls._read = cls.read

        def _read(self, *args, **kwargs):
            _df = self._read(*args, **kwargs)
            error_msg = f"""{cls.__name__}: Schema mismatch:
            Fields: {[f.name for f in set(_df.schema) - set(self.schema)]} are not in the schema.
            Did you mean one of these: {[f.name for f in set(self.schema) - set(_df.schema)]}
            """
            assert sorted(self.columns) == sorted(_df.columns), error_msg
            return _df

        cls.read = _read

    def read(self) -> DataFrame:
        return self.spark.table(self.fqn)


class MyTable(Table):
    fqn = "my_database.my_table"

    name = T.StructField("name", T.StringType(), False)
    age = T.StructField("age", T.IntegerType(), True)
    city = T.StructField("city", T.StringType(), True)

    def read(self):
        return self.spark.createDataFrame([("Alice", 34, "Amsterdam")], self.schema)


table = MyTable()
df = table.read()
df.show()
print(table.name)
print(table.name_type)
print(table.schema)
