import pytest

from examples.stateful_tests.JobTester import JobTester, JobTestFixture


class TestRunnerWithCorrectImplementation(JobTester):
    state = {}

    @pytest.fixture(scope="session")
    def fixture(self):
        return JobTestFixture("test")


class TestRunnerWithCorrectImplementation1(JobTester):
    state = {}

    @pytest.fixture(scope="class")
    def fixture(self):
        return JobTestFixture("another_test")


class TestRunnerWithCorrectImplementation2(JobTester):
    state = {}

    @pytest.fixture(scope="class")
    def fixture(self):
        return JobTestFixture("yet_another_test")
