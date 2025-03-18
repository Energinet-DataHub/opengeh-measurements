import uuid

import pytest

from tests.subsystem_tests.demo.base import BaseJobFixture, BaseJobTests


@pytest.fixture(scope="session")
def job_fixture() -> BaseJobFixture:
    id = f"capacity-settlement-{uuid.uuid4()}"
    print(f"########## FIXTURE {id} ##########")
    return BaseJobFixture(id)


class TestCapacitySettlementJobTests(BaseJobTests):
    @pytest.mark.order(3)
    def test__when_something_else(self, job_fixture: BaseJobFixture) -> None:
        print(job_fixture.id)
