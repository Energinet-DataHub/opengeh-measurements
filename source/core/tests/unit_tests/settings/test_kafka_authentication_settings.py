import os

from core.settings import KafkaAuthenticationSettings


def test__submitted_transactions_stream_settings__should_create_submitted_transactions_from_env():
    # Arrange
    expected_event_hub_namespace = "some event_hub_namespace"
    expected_event_hub_instance = "some event_hub_instance"
    expected_tenant_id = "some tenant_id"
    expected_spn_app_id = "some spn_app_id"
    expected_spn_app_secret = "some spn_app_secret"

    os.environ["EVENT_HUB_NAMESPACE"] = expected_event_hub_namespace
    os.environ["EVENT_HUB_INSTANCE"] = expected_event_hub_instance
    os.environ["TENANT_ID"] = expected_tenant_id
    os.environ["SPN_APP_ID"] = expected_spn_app_id
    os.environ["SPN_APP_SECRET"] = expected_spn_app_secret

    # Act
    actual = KafkaAuthenticationSettings()

    # Assert
    assert actual.event_hub_namespace == expected_event_hub_namespace
    assert actual.event_hub_instance == expected_event_hub_instance
    assert actual.tenant_id == expected_tenant_id
    assert actual.spn_app_id == expected_spn_app_id
    assert actual.spn_app_secret == expected_spn_app_secret
