from dataclasses import dataclass
from pathlib import Path

import yaml


class TestSessionConfiguration:
    """Customizable settings for a test session."""

    # Pytest test classes will fire a warning if it has a constructor (__init__).
    # To avoid a class being treated as a test class set the attribute __test__  to False.
    __test__ = False

    def __init__(self, configuration: dict):
        self.scenario_tests = ScenarioTestsConfiguration(configuration.get("scenario_tests", {}))

    @staticmethod
    def load(path: Path) -> "TestSessionConfiguration":
        """Load settings from a YAML file."""
        if not path.exists():
            return TestSessionConfiguration({})

        with path.open() as stream:
            settings = yaml.safe_load(stream)
            return TestSessionConfiguration(settings)


@dataclass
class ScenarioTestsConfiguration:
    """Settings for scenario tests."""

    def __init__(self, configuration: dict):
        self.show_actual_and_expected = configuration.get("show_actual_and_expected", False)
        self.show_columns_when_actual_and_expected_are_equal = configuration.get(
            "show_columns_when_actual_and_expected_are_equal", False
        )
        self.show_actual_and_expected_count = configuration.get("show_actual_and_expected_count", False)
        self.ignore_extra_columns_in_actual = configuration.get("ignore_extra_columns_in_actual", True)
        self.testing_decorator_enabled = configuration.get("testing_decorator_enabled", False)
        self.testing_decorator_max_rows = configuration.get("testing_decorator_max_rows", 50)
