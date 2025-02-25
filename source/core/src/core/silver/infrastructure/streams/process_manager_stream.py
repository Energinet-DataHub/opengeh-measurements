from pyspark.sql import DataFrame

from core.settings.catalog_settings import CatalogSettings
from core.settings.kafka_authentication_settings import KafkaAuthenticationSettings  # todo: simplify import
from core.settings.storage_account_settings import StorageAccountSettings
from core.utility.shared_helpers import get_checkpoint_path


class ProcessManagerStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = KafkaAuthenticationSettings().create_kafka_options()  # type: ignore
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
        self.silver_container_name = CatalogSettings().silver_container_name  # type: ignore

    def write_stream(
        self,
        dataframe: DataFrame,
    ):
        checkpoint_location = get_checkpoint_path(
            self.data_lake_settings, self.silver_container_name, "processed_transactions"
        )
        event_hub_instance = KafkaAuthenticationSettings().event_hub_instance  # type: ignore

        dataframe.writeStream.format("kafka").options(**self.kafka_options).option("topic", event_hub_instance).option(
            "checkpointLocation", checkpoint_location
        ).trigger(availableNow=True).start()
