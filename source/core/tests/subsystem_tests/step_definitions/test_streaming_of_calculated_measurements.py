from pytest_bdd import given, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
from tests.subsystem_tests.builders.calculated_measurements_row_builder import (
    CalculatedMeasurementsRow,
    CalculatedMeasurementsRowBuilder,
)
from tests.subsystem_tests.fixtures.calculated_measurements_fixture import (
    CalculatedMeasurementsFixture,
)
from tests.subsystem_tests.fixtures.gold_layer_fixture import GoldLayerFixture

scenarios("../features/streaming_of_calculated_measurements.feature")


@given("new valid calculated measurements", target_fixture="calculated_measurements_row")
def _() -> CalculatedMeasurementsRow:
    orchestration_instance_id = identifier_helper.generate_random_string()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    return CalculatedMeasurementsRowBuilder().build(
        orchestration_instance_id=orchestration_instance_id, metering_point_id=metering_point_id
    )


@when("inserted into the calculated measurements table")
def _(calculated_measurements_row: CalculatedMeasurementsRow) -> None:
    calculated_measurements_fixture = CalculatedMeasurementsFixture()
    calculated_measurements_fixture.insert_calculated_measurements(calculated_measurements_row)


@then("the calculated measurements are available in the Gold Layer")
def _(calculated_measurements_row: CalculatedMeasurementsRow, gold_layer_fixture: GoldLayerFixture) -> None:
    gold_layer_fixture.assert_measurement_persisted(calculated_measurements_row.orchestration_instance_id)


@then("the calculated measurement transaction is available in the SAP Series Gold table")
def _(calculated_measurements_row: CalculatedMeasurementsRow, gold_layer_fixture: GoldLayerFixture) -> None:
    gold_layer_fixture.assert_sap_series_persisted(calculated_measurements_row.metering_point_id)
