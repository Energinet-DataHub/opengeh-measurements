from pyspark.sql import DataFrame

from core.settings.kafka_authentication_settings import KafkaAuthenticationSettings  # todo: simplify import
from core.settings.silver_settings import SilverSettings
from core.settings.storage_account_settings import StorageAccountSettings
from core.utility.shared_helpers import get_checkpoint_path


class ProcessManagerStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = KafkaAuthenticationSettings().create_kafka_options()
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.silver_container_name = SilverSettings().silver_container_name

    def write_stream(
        self,
        dataframe: DataFrame,
    ):
        checkpoint_location = get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, "processed_transactions"
        )
        event_hub_instance = KafkaAuthenticationSettings().event_hub_instance

        dataframe.writeStream.format("kafka").options(**self.kafka_options).option("topic", event_hub_instance).option(
            "checkpointLocation", checkpoint_location
        ).trigger(availableNow=True).start()
