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


def set_test_environment_variables() -> None:
    os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"] = "app_conn_str"
    os.environ["CATALOG_NAME"] = "spark_catalog"
    os.environ["BRONZE_CONTAINER_NAME"] = "bronze"
    os.environ["SILVER_CONTAINER_NAME"] = "silver"
    os.environ["GOLD_CONTAINER_NAME"] = "gold"
    os.environ["CORE_INTERNAL_CONTAINER_NAME"] = "core_internal"
    os.environ["BRONZE_DATABASE_NAME"] = "measurements_bronze"
    os.environ["SILVER_DATABASE_NAME"] = "measurements_silver"
    os.environ["GOLD_DATABASE_NAME"] = "measurements_gold"
    os.environ["CALCULATED_DATABASE_NAME"] = "calculated_database_name"
    os.environ["CORE_INTERNAL_DATABASE_NAME"] = "measurements_core_internal"
    os.environ["EVENT_HUB_NAMESPACE"] = "event_hub_namespace"
    os.environ["EVENT_HUB_INSTANCE"] = "event_hub_instance"
    os.environ["TENANT_ID"] = "tenant_id"
    os.environ["SPN_APP_ID"] = "spn_app_id"
    os.environ["SPN_APP_SECRET"] = "spn_app_secret"
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = "datalake"
    os.environ["CONTINUOUS_STREAMING_ENABLED"] = "false"
    os.environ["DATABRICKS_WORKSPACE_URL"] = "workspace-url"
    os.environ["DATABRICKS_TOKEN"] = "token"
    os.environ["DATABRICKS_JOBS"] = "job1,job2"
