from decimal import Decimal

import geh_common.testing.dataframes.assert_schemas as assert_schemas
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import core.silver.domain.transformations.migrations_transformation as mit
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames
from core.silver.domain.constants.enums.metering_point_type_dh2_enum import MeteringPointTypeDH2
from tests.helpers.builders.migrated_transactions_builder import MigratedTransactionsBuilder
from tests.helpers.schemas.silver_measurements_schema import silver_measurements_schema


def test__transform__should_return_expected_schema(spark: SparkSession) -> None:
    # Arrange
    migrated_transactions = MigratedTransactionsBuilder(spark).add_row().build()

    # Act
    actual = mit.transform(migrated_transactions)

    # Assert
    assert_schemas.assert_schema(actual.schema, silver_measurements_schema, ignore_nullability=True)


def test__transform__should_return_correct_decimal_value(spark: SparkSession) -> None:
    # Arrange
    expected_decimal_value = Decimal(1.5)
    migrated_transactions = (
        MigratedTransactionsBuilder(spark).add_row(values=[(0, "D01", expected_decimal_value)]).build()
    )

    # Act
    actual = mit.transform(migrated_transactions)

    # Assert
    assert actual.collect()[0].points[0].quantity == expected_decimal_value


def test__transform__should_not_contain_dh2_metering_point_types(spark: SparkSession) -> None:
    # Arrange
    dh2_mpts = [e.value for e in MeteringPointTypeDH2]

    migrated_transactions = MigratedTransactionsBuilder(spark)
    for i, mpt in enumerate(dh2_mpts):
        migrated_transactions.add_row(type_of_mp=mpt, metering_point_id=str(i))
    migrated_transactions = migrated_transactions.build()

    # Act
    actual = mit.transform(migrated_transactions)

    # Assert
    remaining_dh2_mpts = actual.filter(col(SilverMeasurementsColumnNames.metering_point_type).isin(dh2_mpts)).collect()
    assert remaining_dh2_mpts == []
