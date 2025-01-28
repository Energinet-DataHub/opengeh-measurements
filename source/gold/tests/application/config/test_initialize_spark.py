from pyspark.sql.session import SparkSession

from opengeh_gold.application.config.spark import initialize_spark


def test__initialize_spark__returns_spark_session():
    # Act
    actual = initialize_spark()
    # Assert
    assert isinstance(actual, SparkSession)
