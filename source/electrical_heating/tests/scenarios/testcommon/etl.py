from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

"""
README:
- This is the framework (module `etl` of package `opengeh-testcommon`) for writing tests for ETL processes.
- The API is enlisted below.
- Other functionality is:
    - A tool to update all test modules in a directory including subdirectories.
    - Documentation on how to write tests (test file, folders, fixture, ...)
    - CSV column where name begins with "#" is ignored.
    - Test configuration (as in wholesale implementation) is supported
"""


# Consider extracting actual and expected in the test itself and then simplify to:
# Was: assert_output(actual_and_expected, output_name)
def assert_dataframes(actual: DataFrame, expected: DataFrame):
    pass

# Was get_output_names
def get_then_names() -> list[str]:
    return []

def create_dataframe_from_csv(csv_path: str, dataframe_schema: StructType) -> DataFrame:
    pass
