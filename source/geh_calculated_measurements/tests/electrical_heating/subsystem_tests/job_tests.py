import uuid

import pytest


class BaseJobTests:
    def setup_fixture(self, fixture) -> None:
        self.fixture = fixture

    @pytest.mark.order(1)
    def test__given_job_input(self) -> None:
        # Act
        self.fixture.job_state.calculation_input.job_id = self.fixture.get_job_id()
        self.fixture.job_state.calculation_input.orchestration_instance_id = uuid.uuid4()

        print(self.fixture.job_state.calculation_input.job_id)
        # Assert
        assert self.fixture.job_state.calculation_input.job_id is not None

    @pytest.mark.order(2)
    def test__given_job_input2(self) -> None:
        # Act
        self.fixture.job_state.calculation_input.job_id = self.fixture.get_job_id()
        self.fixture.job_state.calculation_input.orchestration_instance_id = uuid.uuid4()

        print(self.fixture.job_state.calculation_input.job_id)
        # Assert
        assert self.fixture.job_state.calculation_input.job_id is not None
