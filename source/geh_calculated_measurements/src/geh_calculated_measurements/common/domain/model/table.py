from abc import ABC

import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


class Table(ABC):
    schema: T.StructType
    spark: SparkSession
    columns: list[str]
    nullable = True
    _df: DataFrame

    def __init__(self, df: DataFrame) -> None:
        assert hasattr(self, "schema"), "Table must have a schema."
        assert hasattr(self, "spark"), "Table must have a spark session."

    def __init_subclass__(cls) -> None:
        """Will set the schema and columns attributes of the subclass."""
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
