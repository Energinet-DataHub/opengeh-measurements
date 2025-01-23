# Copyright 2020 Energinet DataHub A/S
#
# Licensed under the Apache License, Version 2.0 (the "License2");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# Variables defined in the infrastructure repository (https://github.com/Energinet-DataHub/dh3-infrastructure)
from pyspark import SparkConf
from pyspark.sql.session import SparkSession

from bronze.application.settings.submitted_transactions_stream_settings import (
    SubmittedTransactionsStreamSettings,
)


def submit_transactions() -> None:
    kafka_options = SubmittedTransactionsStreamSettings().create_kafka_options()

    spark = initialize_spark()

    spark.readStream.format("kafka").options(**kafka_options).load().writeStream.format("delta").option(
        "checkpointLocation", "checkpointReceiving"
    ).toTable("submitted_transactions")


def initialize_spark() -> SparkSession:
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
