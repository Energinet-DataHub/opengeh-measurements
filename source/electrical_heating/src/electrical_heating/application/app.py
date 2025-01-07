from src.electrical_heating.app_runner import AppInterface
from src.electrical_heating.application.electrical_heating_args import (
    ElectricalHeatingArgs,
)
from src.electrical_heating.domain import calculation
from src.electrical_heating.infrastructure.spark_initializor import initialize_spark


class ElectricalHeatingApp(AppInterface):
    settings: ElectricalHeatingArgs

    def __init__(self, settings: ElectricalHeatingArgs):
        self.settings = settings

    def run(self):
        spark = initialize_spark()
        calculation.execute(spark, self.settings)
