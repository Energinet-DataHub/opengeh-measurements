from pyspark.sql import SparkSession

from opengeh_bronze.domain.transformations.bronze_measurements_transformations import unpack_proto


def test__unpack_proto__when_called__then_return_dataframe_with_measurement_and_properties_columns(spark: SparkSession):
    # Arrange
    data = [
        ("value", "properties"),
    ]
    df = spark.createDataFrame(data, ["value", "properties"])

    # Act
    result = unpack_proto(df)

    # Assert
    assert result.columns == [
        "version",
        "orchestration_instance_id",
        "orchestratioon_type",
        "metering_point_id",
        "transaction_id",
        "transaction_creation_datetime",
        "start_datetime",
        "end_datetime",
        "metering_point_type",
        "product",
        "unit",
        "resolution",
        "points",
        "properties",
    ]
    assert result.count() == 1
    assert result.collect()[0].value == "value"
    assert result.collect()[0].properties == "properties"
    assert result.collect()[0].version is None
    assert result.collect()[0].orchestration_instance_id is None
    assert result.collect()[0].orchestratioon_type is None
    assert result.collect()[0].metering_point_id is None
    assert result.collect()[0].transaction_id is None
    assert result.collect()[0].transaction_creation_datetime is None
    assert result.collect()[0].start_datetime is None
    assert result.collect()[0].end_datetime is None
    assert result.collect()[0].metering_point_type is None
    assert result.collect()[0].product is None
    assert result.collect()[0].unit is None
    assert result.collect()[0].resolution is None
    assert result.collect()[0].points is None
    spark.stop()
