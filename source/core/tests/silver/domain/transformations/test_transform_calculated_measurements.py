from silver.builders.bronze_calculated_measurements_builder import BronzeMeasurementsDataFrameBuilder
from silver.schemas.silver_measurements_schema import silver_measurements_schema

from core.silver.domain.transformations.transform_calculated_measurements import transform_calculated_measurements


def test__transform_calculated_measurements__should_return_transformed_df(spark):
    # Arrange
    bronze_calculated_df = BronzeMeasurementsDataFrameBuilder(spark).add_row().build()
    expected_schema = silver_measurements_schema

    # Act
    result = transform_calculated_measurements(bronze_calculated_df)

    # Assert
    assert result.count() == 1
    assert result.columns == [field.name for field in expected_schema.fields]
