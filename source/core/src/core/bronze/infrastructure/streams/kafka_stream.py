from pyspark.sql import DataFrame, SparkSession

import core.utility.path_helper as path_helper
from core.bronze.domain.constants import BronzeTableNames
from core.bronze.infrastructure.config.storage_container_names import StorageContainerNames
from core.bronze.infrastructure.settings import (
    KafkaAuthenticationSettings,
    StorageAccountSettings,
    SubmittedTransactionsStreamSettings,
)
from core.settings.catalog_settings import CatalogSettings


class KafkaStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = KafkaAuthenticationSettings().create_kafka_options()  # type: ignore
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore
        self.bronze_database_name = CatalogSettings().bronze_database_name  # type: ignore

    def submit_transactions(self, spark: SparkSession) -> None:
        checkpoint_location = path_helper.get_checkpoint_path(
            self.data_lake_settings, StorageContainerNames.bronze, "submitted_transactions"
        )
        write_stream = (
            spark.readStream.format("kafka")
            .options(**self.kafka_options)
            .load()
            .writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint_location)
        )

        stream_settings = SubmittedTransactionsStreamSettings()  # type: ignore

        if stream_settings.continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        write_stream.toTable(f"{self.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}")

    def write_stream(
        self,
        dataframe: DataFrame,
    ):
        checkpoint_location = path_helper.get_checkpoint_path(
            self.data_lake_settings, StorageContainerNames.bronze, "processed_transactions"
        )
        event_hub_instance = KafkaAuthenticationSettings().event_hub_instance  # type: ignore

        dataframe.writeStream.format("kafka").options(**self.kafka_options).option("topic", event_hub_instance).option(
            "checkpointLocation", checkpoint_location
        ).trigger(availableNow=True).start()
