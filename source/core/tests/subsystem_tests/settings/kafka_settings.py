from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).parent.parent


class KafkaSettings(BaseSettings):
    """
    Contains the environment configuration for the tests.
    This class must be included when running tests in CD.
    """

    event_hub_namespace: str = Field(alias="KAFKA_EVENT_HUB_NAMESPACE")
    event_hub_submitted_transactions_instance: str = Field(alias="KAFKA_EVENT_HUB_SUBMITTED_TRANSACTIONS_INSTANCE")
    event_hub_receipt_instance: str = Field(alias="KAFKA_EVENT_HUB_RECEIPT_INSTANCE")

    model_config = SettingsConfigDict(
        env_file=f"{PROJECT_ROOT}/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
