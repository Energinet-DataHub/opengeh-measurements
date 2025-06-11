from pytest_bdd import given, scenarios, then, when

import tests.helpers.identifier_helper as identifier_helper
from tests.subsystem_tests.builders.migrated_measurements_row_builder import (
    MigratedMeasurementsRow,
    MigratedMeasurementsRowBuilder,
)
from tests.subsystem_tests.fixtures.gold_layer_fixture import GoldLayerFixture
from tests.subsystem_tests.fixtures.migrated_measurements_fixture import (
    MigratedMeasurementsFixture,
)

scenarios("../features/streaming_of_migrated_measurements.feature")


@given(
    "a new valid migrated transaction inserted into the Migration Silver table",
    target_fixture="migrated_measurements_row",
)
def _() -> MigratedMeasurementsRow:
    transaction_id = identifier_helper.generate_random_string()
    metering_point_id = identifier_helper.create_random_metering_point_id()
    return MigratedMeasurementsRowBuilder().build(transaction_id=transaction_id, metering_point_id=metering_point_id)


@when("streaming from Migration silver to Measurements gold")
def _(migrated_measurements_row: MigratedMeasurementsRow) -> None:
    migrated_measurements_fixture = MigratedMeasurementsFixture()
    migrated_measurements_fixture.insert_migrated_measurements(migrated_measurements_row)
    migrated_measurements_fixture.start_migrations_to_measurements_job()


@then("the migrated transaction is available in the Gold layer")
def _(migrated_measurements_row: MigratedMeasurementsRow, gold_layer_fixture: GoldLayerFixture) -> None:
    gold_layer_fixture.assert_migrated_measurement_persisted(migrated_measurements_row.transaction_id)


@then("the migrated transaction is available in the Gold SAP Series table")
def _(migrated_measurements_row: MigratedMeasurementsRow, gold_layer_fixture: GoldLayerFixture) -> None:
    gold_layer_fixture.assert_sap_series_persisted(migrated_measurements_row.metering_point_id)
