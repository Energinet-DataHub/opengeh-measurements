import pytest

from examples.stateful_tests.JobTester import JobTester, JobTestFixture


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithCorrectImplementation(JobTester):
    @pytest.fixture(scope="session")
    def fixture(self):
        return JobTestFixture("test_wrong")


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithCorrectImplementation1(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        return JobTestFixture("another_test_wrong")


@pytest.mark.xfail(reason="These tests are flaky as they manipulate the same object. Uncomment to see what happens.")
class TestRunnerWithCorrectImplementation2(JobTester):
    @pytest.fixture(scope="class")
    def fixture(self):
        return JobTestFixture("yet_another_test_wrong")
