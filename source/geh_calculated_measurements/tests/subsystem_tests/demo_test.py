import uuid

import pytest


class BaseJobFixture:
    def __init__(self, id: str) -> None:
        self.id = f"########## {id} ##########"


def capacity_settlement_job_fixture() -> BaseJobFixture:
    return BaseJobFixture(f"capacity-settlement-{uuid.uuid4()}")


def electrical_heating_job_fixture() -> BaseJobFixture:
    return BaseJobFixture(f"electrical-heating-{uuid.uuid4()}")


@pytest.mark.parametrize("job_fixture", [capacity_settlement_job_fixture(), electrical_heating_job_fixture()])
class TestJobTests:
    @pytest.mark.order(1)
    def test__given_job_input(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)

    @pytest.mark.order(2)
    def test__when_job_is_started(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)
