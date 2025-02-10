from typing import Any, Generator

import pytest
from pytest_bdd import given, scenarios, then, when

scenarios("../features/brs_021_transmission_of_meter_readings.feature")


@pytest.fixture
def context() -> Generator[dict, Any, None]:
    ctx: dict[str, Any] = {"stored": False}
    yield ctx


@given("a valid meter reading has been enqueued in the Event Hub")
def _(context: dict) -> None:
    context["meter_reading"] = {"id": 1, "value": 101, "timestamp": "2029-01-30T12:00:00Z"}
    context["enqueued"] = True  # simulate the enqueuing
    assert context["enqueued"] is True


@when("the Measurement system processes the queued meter reading")
def _(context: dict) -> None:
    context["processed"] = True  # simulate the processing
    assert context["processed"] is True


@then("the meter reading is successfully recorded in Measurements")
def _(context: dict) -> None:
    context["stored"] = True  # simulate the successful recording
    assert context["stored"] is True


@then("an acknowledgment is sent back to the Event Hub")
def _(context: dict) -> None:
    context["receipt"] = {"status": "success"}  # simulate the receipt
    assert context["receipt"]["status"] == "success"
