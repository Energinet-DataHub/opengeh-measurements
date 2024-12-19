"""
TODO: Move to opengeh-testcommon
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame

@dataclass
class TestCase:
    name: str
    actual: DataFrame
    expected: DataFrame

@dataclass
class TestCases:
    def __init__(self, test_cases: list[TestCase]) -> None:
        self.test_cases = test_cases

    def __getitem__(self, key: str) -> TestCase:
        return next(test_case for test_case in self.test_cases if test_case.name == key)
