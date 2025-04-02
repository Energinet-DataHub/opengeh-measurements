import pytest

from examples.stateful_tests.ExampleTester import ExampleTester, ExampleTestFixture


class TestRunnerWithCorrectImplementation(ExampleTester):
    state = {}

    @pytest.fixture(scope="session")
    def fixture(self):
        return ExampleTestFixture("test")


class TestRunnerWithCorrectImplementation1(ExampleTester):
    state = {}

    @pytest.fixture(scope="class")
    def fixture(self):
        return ExampleTestFixture("another_test")


class TestRunnerWithCorrectImplementation2(ExampleTester):
    state = {}

    @pytest.fixture(scope="class")
    def fixture(self):
        return ExampleTestFixture("yet_another_test")
