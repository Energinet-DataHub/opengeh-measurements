import uuid

import pytest

from tests.subsystem_tests.demo.base import BaseJobFixture, BaseJobTests


@pytest.fixture(scope="session")
# def electrical_heating_job_fixture() -> BaseJobFixture:
def job_fixture() -> BaseJobFixture:
    id = f"electrical-heating-{uuid.uuid4()}"
    print(f"########## FIXTURE {id} ##########")
    return BaseJobFixture(id)


# @pytest.mark.parametrize("job_fixture", [electrical_heating_job_fixture()])
class TestElectricalHeatingJobTests(BaseJobTests):
    @pytest.mark.order(3)
    def test__when_job_is_finished(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)
