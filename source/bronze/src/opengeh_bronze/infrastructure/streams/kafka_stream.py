from pyspark.sql import DataFrame, SparkSession

import opengeh_bronze.infrastructure.helpers.path_helper as path_helper
from opengeh_bronze.domain.constants.database_names import DatabaseNames
from opengeh_bronze.domain.constants.table_names import TableNames
from opengeh_bronze.infrastructure.config.storage_container_names import StorageContainerNames
from opengeh_bronze.infrastructure.settings import (
    KafkaAuthenticationSettings,
    StorageAccountSettings,
    SubmittedTransactionsStreamSettings,
)


class KafkaStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = KafkaAuthenticationSettings().create_kafka_options()  # type: ignore
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT  # type: ignore

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

        write_stream.toTable(f"{DatabaseNames.bronze_database}.{TableNames.bronze_submitted_transactions_table}")

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
