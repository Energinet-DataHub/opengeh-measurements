import time
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pytest_bdd import given, scenarios, then, when

from tests.bronze.conftest import Generator
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
    value = ValueBuilder(spark).add_row().build()
    context["value"] = value


@when("the measurement transaction is enqueued in the Event Hub")
def _(context: dict[str, Any]) -> None:
    print("DEBUG: Sending event with value: {}".format(context["value"]))
    context["fixture"].send_submitted_transactions_event(context["value"])
    print("DEBUG: done with enqueue")


@then("an acknowledgement is sent to the Event Hub")
def _(context: dict[str, Any]) -> None:
    pass
    start_time = time.time()
    timeout = 120
    poll_interval = 10

    while time.time() - start_time < timeout:
        print("DEBUG: Waiting for receipt")
        context["fixture"].assert_receipt()
        print("Sleeping...")
        time.sleep(poll_interval)
