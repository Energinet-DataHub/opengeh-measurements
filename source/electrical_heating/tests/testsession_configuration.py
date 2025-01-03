from dataclasses import dataclass


class TestSessionConfiguration:

    # Pytest test classes will fire a warning if it has a constructor (__init__).
    # To avoid a class being treated as a test class set the attribute __test__  to False.
    __test__ = False

    def __init__(self, configuration: dict):
        self.scenario_tests = _create_scenario_tests_configuration(
            configuration["scenario_tests"]
        )


@dataclass
class ScenarioTestsConfiguration:
    show_actual_and_expected: bool
    show_columns_when_actual_and_expected_are_equal: bool
    show_actual_and_expected_count: bool


def _create_scenario_tests_configuration(configuration: dict) -> ScenarioTestsConfiguration:
    configuration = configuration or {}

    return ScenarioTestsConfiguration(
        show_actual_and_expected=configuration.get("show_actual_and_expected", False),
        show_columns_when_actual_and_expected_are_equal=configuration.get("show_columns_when_actual_and_expected_are_equal", False),
        show_actual_and_expected_count=configuration.get("show_actual_and_expected_count", False),
    )
