import abc
from dataclasses import dataclass

import pytest
from databricks.sdk.service.jobs import RunResultState


@dataclass
class StateMock:
    result_state: RunResultState


@dataclass
class ResultMock:
    name: str
    state: StateMock


class ExampleTestFixture:
    def __init__(self, name):
        self.name = name

    def run_job(self):
        return ResultMock(self.name, StateMock(RunResultState.SUCCESS))

    def wait_for_job_completion(self):
        return ResultMock(self.name, StateMock(RunResultState.SUCCESS))


class ExampleTester(abc.ABC):
    state = {}

    def __init_subclass__(cls):
        """Reset the state dictionary for each subclass.

        This method is called when a subclass is created. It resets the state
        dictionary for each subclass. Without this method, the state dictionary
        would be shared between all subclasses of JobTester, which could lead
        to unexpected behavior in pytest.

        Try to avoid using class variables in tests, as they can lead to
        flaky tests.
        """
        # Uncomment `cls.state = {}` to see the flaky behavior.
        #
        cls.state = {}
        return super().__init_subclass__()

    @pytest.fixture(scope="class")
    @abc.abstractmethod
    def fixture(self) -> ExampleTestFixture:
        raise NotImplementedError("The fixture method must be implemented.")

    @pytest.mark.order(1)
    def test__fixture_is_correctly_made(self, fixture: ExampleTestFixture):
        assert fixture is not None, "The fixture was not created successfully."
        assert isinstance(fixture, ExampleTestFixture), "The fixture is not of the correct type."

    @pytest.mark.order(2)
    def test__job_starts(self, fixture: ExampleTestFixture):
        job = fixture.run_job()
        self.state = self.state.update({"job": job})
        assert job.name == fixture.name, "The job name does not match the fixture name."
        assert job.state.result_state is not None, "The job did not return a RunResultState."
        assert job.state.result_state == RunResultState.SUCCESS, f"The job did not complete successfully: {job}"

    @pytest.mark.order(3)
    def test__job_completion(self, fixture: ExampleTestFixture):
        job = self.state["job"]
        assert job.name == fixture.name, "The job name does not match the fixture name."
        assert job.state.result_state is not None, "The job did not return a RunResultState."
        assert job.state.result_state == RunResultState.SUCCESS, f"The job did not complete successfully: {job}"
