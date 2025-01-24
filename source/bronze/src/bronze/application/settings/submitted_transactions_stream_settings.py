from pydantic_settings import BaseSettings


class SubmittedTransactionsStreamSettings(BaseSettings):
    event_hub_namespace: str
    event_hub_instance: str
    tenant_id: str
    spn_app_id: str
    spn_app_secret: str

    class Config:
        case_sensitive = False

    def create_kafka_options(self) -> dict:
        return {
            "kafka.bootstrap.servers": f"{self.event_hub_namespace}.servicebus.windows.net:9093",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{self.spn_app_id}" clientSecret="{self.spn_app_secret}" scope="https://{self.event_hub_namespace}.servicebus.windows.net/.default" ssl.protocol="SSL";',
            "kafka.sasl.oauthbearer.token.endpoint.url": f"https://login.microsoft.com/{self.tenant_id}/oauth2/v2.0/token",
            "subscribe": self.event_hub_instance,
            "startingOffsets": "latest",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler",
        }
