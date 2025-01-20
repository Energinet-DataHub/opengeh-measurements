# TODO AJW: This is a copy of the function from the wholesale codebase.
# This should be moved to a shared location when the time comes.

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


def read_from_csv(
    spark: SparkSession,
    file_name: str,
    sep: str = ";",
    schema: StructType = None,
) -> DataFrame:
    return spark.read.csv(file_name, header=True, sep=sep, schema=schema)


def write_dataframe_to_table(df: DataFrame, database_name: str, table_name: str, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).saveAsTable(f"{database_name}.{table_name}")


def create_delta_table(
    spark: SparkSession,
    database_name: str,
    table_name: str,
    table_location: str,
    schema: StructType,
) -> None:
    print(f"{database_name}.{table_name} write")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    sql_schema = _struct_type_to_sql_schema(schema)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {database_name}.{table_name} ({sql_schema}) USING DELTA LOCATION '{table_location}'"
    )


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
