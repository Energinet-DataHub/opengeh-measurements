import pytest

from examples.stateful_tests.ExampleTester import ExampleTester, ExampleTestFixture


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithWrongImplementation(ExampleTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        return ExampleTestFixture("test_wrong")


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithWrongImplementation1(ExampleTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        return ExampleTestFixture("another_test_wrong")


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithWrongImplementation2(ExampleTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        return ExampleTestFixture("yet_another_test_wrong")
