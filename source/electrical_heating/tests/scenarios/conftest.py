from pathlib import Path
from typing import Tuple

import pytest
from pyspark.sql import DataFrame, SparkSession

from scenarios.calculator import execute


# IMPORTANT: Must me scoped to module to ensure that each scenario is executed independently
@pytest.fixture(scope="module")
def all_actual_and_expected(spark: SparkSession) -> list[Tuple[str, DataFrame, DataFrame]]:
    scenario_path = str(Path(request.module.__file__).parent)

    # Setup mocks for reads
    reader = Mock(Reader)
    reader.read("input_table").return_value = test_common.create_dataframe_from_csv(scenario_path // "input_table", input_table_schema)

    # If using DI(?)
    container.reader = reader

    # Execute
    result = execute(spark)
    df = result.output_table_1.withColumn()

    # Create return object
    test_case_names = ["output_table_1", "output_table_2"]
    actuals = [result.output_table_1, result.output_table_2]
    expected = [
        test_common.create_dataframes_from_csv(scenario_path // "output_table_1", result.output_table_1.schema),
        test_common.create_dataframes_from_csv(scenario_path // "output_table_2", result.output_table_2.schema),
    ]

    return zip(test_case_names, actuals, expected)
