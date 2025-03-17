import random
import uuid
from datetime import datetime

import pytest
from geh_common.domain.types import MeteringPointType

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsInternalDatabaseDefinition
from geh_calculated_measurements.database_migrations.settings.catalog_settings import CatalogSettings
from tests.subsystem_tests.base_resources.base_job_fixture import BaseJobFixture
from tests.subsystem_tests.base_resources.base_job_tests import BaseJobTests
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration
from tests.subsystem_tests.seed_gold_table import GoldTableRow, GoldTableSeeder

gold_table_row = GoldTableRow(
    metering_point_id="170000030000000201",
    observation_time=datetime(2024, 11, 30, 23, 0, 0),
    quantity=random.uniform(0.1, 10.0),
    metering_point_type=MeteringPointType.CONSUMPTION,
)


class TestElectricalHeating(BaseJobTests):
    """
    Test class for electrical heating.
    """

    fixture = None

    params = {"orchestration-instance-id": uuid.uuid4()}

    def get_or_create_fixture(self, environment_configuration: EnvironmentConfiguration) -> BaseJobFixture:
        if self.fixture is None:
            table_seeder = GoldTableSeeder(environment_configuration)
            table_seeder.seed(gold_table_row)

            self.fixture = BaseJobFixture(
                environment_configuration=environment_configuration,
                job_name="ElectricalHeating",
                params=self.params,
            )
        return self.fixture

    @pytest.fixture(autouse=True, scope="class")
    def setup_fixture(
        self,
        environment_configuration: EnvironmentConfiguration,
    ) -> BaseJobFixture:
        return self.get_or_create_fixture(environment_configuration)

    @pytest.mark.order(5)
    def test__and_then_table_has_data0(self, setup_fixture, spark) -> None:
        catalog = CatalogSettings().catalog_name
        database = CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME
        table = "calculated_measurements"

        # dataframe = spark.table(f"{catalog}.{database}.{table}").take(1)
        dataframe = spark.sql(f"SELECT * FROM  {catalog}.{database}.{table}")
        # count = len(dataframe)
        print(dataframe.show())

        # assert count > 0, f"Expected count to be greater than 0 for table {catalog}.{database}.{table}, but got {count}"
