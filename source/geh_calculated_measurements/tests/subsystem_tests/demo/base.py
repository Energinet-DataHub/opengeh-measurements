import pytest


class BaseJobFixture:
    def __init__(self, id: str) -> None:
        self.id = f"########## {id} ##########"


class BaseJobTests:
    @pytest.mark.order(1)
    def test__given_job_input(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)

    @pytest.mark.order(2)
    def test__when_job_is_started(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)
