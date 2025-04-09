from abc import ABC, abstractmethod

import pyspark.sql.types as t
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import DataFrame, SparkSession


class Table(ABC):
    fully_qualified_name: str
    schema: t.StructType
    spark: SparkSession
    columns: list[str]

    def __init__(self) -> None:
        # TODO Ask about this
        self.spark = SparkSession.builder.getOrCreate()

        if not hasattr(self, "fully_qualified_name"):
            raise AttributeError("Table must define a fully qualified name.")
        if not hasattr(self, "schema"):
            raise AttributeError("Table must define a schema.")
        if not hasattr(self, "spark"):
            raise AttributeError("Table must define a Spark session.")

    @classmethod
    def __init_subclass__(cls) -> None:
        """Automatically called when a class is subclassed."""
        """cls is a reference to the class object of the subclass being created."""
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

        # Store a reference to the original read method of the subclass before it is overridden.
        # This allows the original read method to be called later, even after it has been replaced
        # by the custom _read method.

        # The idea is to inject a custom read method into the subclass that will
        # perform additional checks and transformations on the DataFrame before returning it.
        #
        # The flow is:
        # When the subclass read is called (cls.read), it calls the base read (_read) which calls the
        # subclass read (cls.read) to performs additional checks and transformations before return the
        # modified dataframe.

        cls._read = cls.read

        def _read(self, *args, **kwargs) -> DataFrame:
            print("here2-------------------------------------------------------")  # noqa: T201

            # Call the original read method of the subclass
            _df = self._read(*args, **kwargs)

            # Assert the actual schema against the contract schema
            assert_contract(_df.schema, self.schema)

            # Only select the columns defined in the respective subclass' schema (stored in cls.columns)
            return _df.select(*self.columns)

        # Replaces the original read method of the subclass with a custom _read method.
        # This is done dynamically at the time the subclass is created.
        cls.read = _read

    @abstractmethod
    def read(self) -> DataFrame:
        print("here3-------------------------------------------------------")  # noqa: T201
        pass
