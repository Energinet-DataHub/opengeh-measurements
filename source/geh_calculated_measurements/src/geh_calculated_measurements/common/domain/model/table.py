from abc import ABC

import pyspark.sql.types as T
from pyspark.sql import DataFrame


class Table(ABC):
    schema: T.StructType
    nullable = True
    _df: DataFrame

    def __init__(self, df: DataFrame) -> None:
        assert hasattr(self, "schema"), "The table must have a schema."
        self._df = df

    def __init_subclass__(cls) -> None:
        """init_subclass method is a special class method that is automatically called when a class is subclassed."""
        """ It allows customization of the behavior of subclasses when they are created."""
        """cls refers to the subclass being created."""

        schema = []

        d = {**cls.__class__.__dict__, **cls.__dict__}

        # Inspect all attributes of the class and its parent classes
        for name, field in d.items():
            if isinstance(field, T.StructField):
                schema.append(field)
                setattr(cls, name, field.name)
                setattr(cls, f"{name}_type", field.dataType)

        cls.schema = T.StructType(schema)

        # TODO Assert scheam with custom assert'er
