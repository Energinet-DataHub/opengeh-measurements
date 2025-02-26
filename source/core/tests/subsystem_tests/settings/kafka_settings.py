from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    """Configuration class inheriting pydantic's BaseSettings to automatically load environmental variable.

    Used to define and validate settings for connecting to the submitted transactions Event Hub.

    Attributes:
    event_hub_namespace (str): The namespace of the Event Hub.
    event_hub_submitted_transactions_instance (str): The specific instance of the submitted transactions Event Hub.
    event_hub_receipt_instance (str): The specific instance of the receipt Event Hub.
    tenant_id (str): The tenant ID for the Azure Active Directory.
    spn_app_id (str): The service principal application ID.
    spn_app_secret (str): The service principal application secret.

    Config:
    case_sensitive (bool): Indicates whether the settings are case-sensitive. Defaults to False.

    Methods:
    create_kafka_options() -> dict:
        Generates a dictionary of Kafka options required to connect to the Event Hub using OAuthBearer authentication.
    """

    event_hub_namespace: str
    event_hub_submitted_transactions_instance: str
    event_hub_receipt_instance: str
    tenant_id: str
    spn_app_id: str
    spn_app_secret: str

    class Config:
        case_sensitive = False

    def create_submitted_transactions_options(self) -> dict:
        return {
            "kafka.bootstrap.servers": f"{self.event_hub_namespace}.servicebus.windows.net:9093",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{self.spn_app_id}" clientSecret="{self.spn_app_secret}" scope="https://{self.event_hub_namespace}.servicebus.windows.net/.default" ssl.protocol="SSL";',
            "kafka.sasl.oauthbearer.token.endpoint.url": f"https://login.microsoft.com/{self.tenant_id}/oauth2/v2.0/token",
            "subscribe": self.event_hub_submitted_transactions_instance,
            "startingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
        }

    def create_receipt_options(self) -> dict:
        return {
            "kafka.bootstrap.servers": f"{self.event_hub_namespace}.servicebus.windows.net:9093",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{self.spn_app_id}" clientSecret="{self.spn_app_secret}" scope="https://{self.event_hub_namespace}.servicebus.windows.net/.default" ssl.protocol="SSL";',
            "kafka.sasl.oauthbearer.token.endpoint.url": f"https://login.microsoft.com/{self.tenant_id}/oauth2/v2.0/token",
            "subscribe": self.event_hub_receipt_instance,
            "startingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
        }
