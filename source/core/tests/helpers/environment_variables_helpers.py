import os


def set_kafka_authentication_settings():
    os.environ["EVENT_HUB_NAMESPACE"] = "event_hub_namespace"
    os.environ["EVENT_HUB_INSTANCE"] = "event_hub_instance"
    os.environ["TENANT_ID"] = "tenant_id"
    os.environ["SPN_APP_ID"] = "spn_app_id"
    os.environ["SPN_APP_SECRET"] = "spn_app_secret"


def set_storage_account_settings():
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = "data_lake_storage_account"


def set_submitted_transactions_stream_settings() -> None:
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "true"
