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

import bronze.infrastructure.environment_variables as env_vars


def submit_transactions() -> None:
    evhns_endpoint = f"{env_vars.get_event_hub_namespace()}.servicebus.windows.net"
    evh_instance = env_vars.get_event_hub_instance()
    client_id = env_vars.get_spn_app_id()
    client_secret = env_vars.get_spn_app_secret()
    sasl_config = f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{client_id}" clientSecret="{client_secret}" scope="https://{evhns_endpoint}/.default" ssl.protocol="SSL";'

    kafka_options = {
        "kafka.bootstrap.servers": f"{evhns_endpoint}:9093",
        "kafka.sasl.jaas.config": sasl_config,
        "kafka.sasl.oauthbearer.token.endpoint.url": f"https://login.microsoft.com/{tenant_id}/oauth2/v2.0/token",
        "subscribe": evh_instance,
        "startingOffsets": "latest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
    }

    spark = initialize_spark()
    spark.readStream.format("kafka").options(**kafka_options).load().writeStream.format(
        "delta"
    ).option("checkpointLocation", "checkpointReceiving").toTable(
        "submitted_transactions"
    )


def initialize_spark() -> SparkSession:
    # Set spark config with the session timezone so that datetimes are displayed consistently (in UTC)
    spark_conf = SparkConf(loadDefaults=True).set("spark.sql.session.timeZone", "UTC")
    return SparkSession.builder.config(conf=spark_conf).getOrCreate()
