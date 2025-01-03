from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def assert_schemas(schema1, schema2) -> None:
    assert schema1 == schema2, _log_error_in_schema(schema1, schema2)


def assert_schemas_without_nullability(schema1: StructType, schema2: StructType) -> None:
    """
    Asserts schemas of two DataFrames ignoring nullability.

    Args:
        schema1 (StructType): First Schema.
        schema2 (StructType): Second Schema.
    """

    assert len(schema1) == len(
        schema2
    ), f"Number of columns are different: {len(schema1)} != {len(schema2)}"

    # Check if corresponding columns have the same column names and data types
    for field1, field2 in zip(schema1, schema2):
        assert (
            field1.name == field2.name
        ), f"Column names are different: {field1.name} != {field2.name}"
        assert (
            field1.dataType == field2.dataType
        ), f"Data type mismatch for column '{field1.name}': {field1.dataType} != {field2.dataType}"


def _log_error_in_schema(schema1, schema2) -> None:
    if schema1 != schema2:
        # Compare names of the columns
        if schema1.names != schema2.names:
            print("Column names are different:")
            print("Schema 1: ", schema1.names)
            print("Schema 2: ", schema2.names)

        # Compare data types of the columns
        for field1, field2 in zip(schema1, schema2):
            if field1.dataType != field2.dataType:
                print(f"Data type mismatch for column '{field1.name}':")
                print(f"Schema 1: {field1.dataType}")
                print(f"Schema 2: {field2.dataType}")

        # Compare whether columns are nullable or not
        for field1, field2 in zip(schema1, schema2):
            if field1.nullable != field2.nullable:
                print(f"Nullable mismatch for column '{field1.name}':")
                print(f"Schema 1: Nullable={field1.nullable}")
                print(f"Schema 2: Nullable={field2.nullable}")

    # Get the fields that are in schema1 but not in schema2
    fields_not_found_in_schema2 = set(schema1.fieldNames()).difference(schema2.fieldNames())

    # Get the fields that are in schema2 but not in schema1
    fields_not_found_in_schema1 = set(schema2.fieldNames()).difference(schema1.fieldNames())

    # Print the fields that are not found in schema2
    if fields_not_found_in_schema2:
        print("Fields not found in schema2:")
        for field in fields_not_found_in_schema2:
            print(field)

    # Print the fields that are not found in schema1
    if fields_not_found_in_schema1:
        print("Fields not found in schema1:")
        for field in fields_not_found_in_schema1:
            print(field)


def assert_datetime_with_col_name(actual: DataFrame, expected: datetime, col_name: str):
    actual_date_time = actual.first()[col_name]
    assert actual_date_time.strftime("%Y-%m-%d %H:%M:%S") == expected.strftime("%Y-%m-%d %H:%M:%S")
