from dataclasses import dataclass
from typing import Any

import pytest
from geh_common.data_products.measurements_calculated import calculated_measurements_v1, missing_measurements_log_v1
from geh_common.testing.dataframes import assert_contract
from pyspark.sql import SparkSession

from tests import SPARK_CATALOG_NAME


@dataclass
class DataProduct:
    database_name: str
    view_name: str
    schema: Any


def _get_expected_data_products() -> list[DataProduct]:
    return [
        DataProduct(
            database_name=calculated_measurements_v1.database_name,
            view_name=calculated_measurements_v1.view_name,
            schema=calculated_measurements_v1.schema,
        ),
        DataProduct(
            database_name=missing_measurements_log_v1.database_name,
            view_name=missing_measurements_log_v1.vuew_name,
            schema=missing_measurements_log_v1.schema,
        ),
    ]


@pytest.mark.parametrize(
    ("data_product"),
    _get_expected_data_products(),
)
def test_data_product_has_expected_schema(
    migrations_executed: None,  # Used implicitly
    spark: SparkSession,
    data_product: DataProduct,
) -> None:
    # Act
    view_df = spark.table(f"{SPARK_CATALOG_NAME}.{data_product.database_name}.{data_product.view_name}").limit(1)

    # Assert
    assert_contract(actual_schema=view_df.schema, contract=data_product.schema)


def _get_all_views_in_catalog(spark: SparkSession) -> list[str]:
    """
    Retrieves all views from all databases in the catalog.
    Returns a list of strings in the format "{database_name}.{view_name}".
    """
    all_databases = spark.sql(f"SHOW DATABASES IN {SPARK_CATALOG_NAME}").collect()
    all_views = []
    for database in all_databases:
        database_name = database.namespace
        views = [
            f"{database_name}.{row.viewName}"
            for row in spark.sql(f"SHOW VIEWS IN {SPARK_CATALOG_NAME}.{database_name}").collect()
        ]
        all_views.extend(views)
    return all_views


def test_all_created_views_are_in_expected_data_products(
    migrations_executed: None,  # Used implicitly
    spark: SparkSession,
) -> None:
    """
    This test ensures that all the views that we create are included in the expected data product list
    """

    # Arrange
    actual_views = _get_all_views_in_catalog(spark)
    expected_views = [
        f"{data_product.database_name}.{data_product.view_name}" for data_product in _get_expected_data_products()
    ]

    # Assert
    assert set(actual_views) == set(expected_views)
