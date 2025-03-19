from typing import Any

import pytest
from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
from tests.conftest import Generator
from tests.helpers.builders.submitted_transactions_builder import ValueBuilder
from tests.subsystem_tests.fixtures.core_fixture import CoreFixture
from tests.subsystem_tests.settings.databricks_settings import DatabricksSettings

scenarios("../features/transmission_of_meter_readings.feature")


@pytest.fixture
def context() -> Generator[dict, Any, None]:
    ctx: dict[str, Any] = {}
    databricks_settings = DatabricksSettings()  # type: ignore
    ctx["fixture"] = CoreFixture(databricks_settings)
    yield ctx


@given("a valid measurement transaction")
def _(spark: SparkSession, context: dict[str, Any]) -> None:
    context["orchestration_instance_id"] = identifier_helper.generate_random_string()
    value = ValueBuilder(spark).add_row(orchestration_instance_id=context["orchestration_instance_id"]).build()
    context["value"] = value


@when("the measurement transaction is enqueued in the Event Hub")
def _(context: dict[str, Any]) -> None:
    context["fixture"].send_submitted_transactions_event(context["value"])


@then("an acknowledgement is sent to the Event Hub")
def _(context: dict[str, Any]) -> None:
    pass
    context["fixture"].assert_receipt(context["orchestration_instance_id"])
