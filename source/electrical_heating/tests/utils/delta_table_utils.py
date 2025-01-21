# TODO AJW: This is a copy of the function from the wholesale codebase.
# This should be moved to a shared location when the time comes.

import os
import shutil

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_from_csv(
    spark: SparkSession,
    file_name: str,
    sep: str = ";",
) -> DataFrame:
    return spark.read.csv(file_name, header=True, sep=sep)


def write_dataframe_to_table(
    df: DataFrame, database_name: str, table_name: str, mode: str = "overwrite"
) -> None:
    df.write.format("delta").mode(mode).saveAsTable(f"{database_name}.{table_name}")


def create_delta_table(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    table_location: str,
    schema: StructType,
) -> None:
    if os.path.exists(table_location) and os.listdir(table_location):
        # Clear the location if it is not empty
        shutil.rmtree(table_location)

    sql_schema = _struct_type_to_sql_schema(schema)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({sql_schema}) USING DELTA LOCATION '{table_location}'"
    )

    print(f"Created Delta table {database_name}.{table_name} at {table_location}.")  # noqa: T201


def create_database(spark: SparkSession, database_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"Created database {database_name}.")  # noqa: T201


def create_catalog(spark: SparkSession, catalog_name: str) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    print(f"Created catalog {catalog_name}.")  # noqa: T201


def _struct_type_to_sql_schema(schema: StructType) -> str:
    schema_string = ""
    for field in schema.fields:
        field_name = field.name
        field_type = field.dataType.simpleString()

        if not field.nullable:
            field_type += " NOT NULL"

        schema_string += f"{field_name} {field_type}, "

    # Remove the trailing comma and space
    schema_string = schema_string.rstrip(", ")
    return schema_string
