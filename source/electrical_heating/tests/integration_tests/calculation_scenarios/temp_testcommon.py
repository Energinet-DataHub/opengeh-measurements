"""
TODO: Move to opengeh-testcommon
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from testcommon.etl import read_csv


@dataclass
class TestCase:
    """
    Represents a test case for a specific scenario.
    The expected data is lazy-loaded from a CSV file as it may not exist in all scenarios.
    """

    def __init__(self, name: str, expected_csv_path: str, actual: DataFrame) -> None:
        self.name = name
        self.expected_csv_path = expected_csv_path
        self.actual = actual

    name: str
    actual: DataFrame

    @property
    def expected(self) -> DataFrame:
        return read_csv(
            self.actual.sparkSession, self.expected_csv_path, self.actual.schema
        )


@dataclass
class TestCases:
    def __init__(self, test_cases: list[TestCase]) -> None:
        self.test_cases = test_cases

    def __getitem__(self, key: str) -> TestCase:
        return next(test_case for test_case in self.test_cases if test_case.name == key)
