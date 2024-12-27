from pyspark.sql import SparkSession
import database_migration.migrations as migrations


def test__migrations(spark: SparkSession):
    # Act
    migrations.migrate()

    # Assert
    assert True
