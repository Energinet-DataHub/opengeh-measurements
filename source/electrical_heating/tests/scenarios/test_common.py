from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


# Consider extracting actual and expected in the test itself and then simplify to:
#    def assert_dataframes(actual: DataFrame, expected: DataFrame)
def assert_output(actual_and_expected, output_name):
    pass

# Rename to "get_then_names"?
def get_output_names() -> list[str]:
    return []

def create_dataframe_from_csv(csv_path: str, dataframe_schema: StructType) -> DataFrame:
    pass
