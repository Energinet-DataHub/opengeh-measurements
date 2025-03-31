from pyspark.sql import SparkSession

import core.utility.shared_helpers as shared_helpers
from core.bronze.infrastructure.config import BronzeTableNames
from core.settings.bronze_settings import BronzeSettings
from core.settings.kafka_authentication_settings import KafkaAuthenticationSettings
from core.settings.storage_account_settings import StorageAccountSettings
from core.settings.streaming_settings import StreamingSettings


class KafkaStream:
    kafka_options: dict

    def __init__(self) -> None:
        self.kafka_options = KafkaAuthenticationSettings().create_kafka_options()
        self.data_lake_settings = StorageAccountSettings().DATALAKE_STORAGE_ACCOUNT
        self.bronze_database_name = BronzeSettings().bronze_database_name
        self.bronze_container_name = BronzeSettings().bronze_container_name

    def submit_transactions(self, spark: SparkSession) -> None:
        checkpoint_location = shared_helpers.get_checkpoint_path(
            self.data_lake_settings, self.bronze_container_name, "submitted_transactions"
        )
        write_stream = (
            spark.readStream.format("kafka")
            .options(**self.kafka_options)
            .load()
            .writeStream.outputMode("append")
            .option("checkpointLocation", checkpoint_location)
        )

        stream_settings = StreamingSettings()

        if stream_settings.continuous_streaming_enabled is False:
            write_stream = write_stream.trigger(availableNow=True)

        write_stream.toTable(f"{self.bronze_database_name}.{BronzeTableNames.bronze_submitted_transactions_table}")
