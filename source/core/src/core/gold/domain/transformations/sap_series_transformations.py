import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from core.gold.domain.constants.column_names.gold_measurements_sap_series_column_names import (
    GoldMeasurementsSAPSeriesColumnNames,
)
from core.silver.domain.constants.column_names.silver_measurements_column_names import (
    SilverMeasurementsColumnNames,
)


def transform(silver_measurements: DataFrame) -> DataFrame:
    """Transform silver measurements transactions to gold series SAP measurements.

    :param silver_measurements: DataFrame containing silver measurements.
    :return: DataFrame with transformed gold series SAP measurements.
    """
    return silver_measurements.select(
        F.col(SilverMeasurementsColumnNames.orchestration_type).alias(
            GoldMeasurementsSAPSeriesColumnNames.orchestration_type
        ),
        F.col(SilverMeasurementsColumnNames.metering_point_id).alias(
            GoldMeasurementsSAPSeriesColumnNames.metering_point_id
        ),
        F.col(SilverMeasurementsColumnNames.transaction_id).alias(GoldMeasurementsSAPSeriesColumnNames.transaction_id),
        F.col(SilverMeasurementsColumnNames.transaction_creation_datetime).alias(
            GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime
        ),
        F.col(SilverMeasurementsColumnNames.start_datetime).alias(GoldMeasurementsSAPSeriesColumnNames.start_time),
        F.col(SilverMeasurementsColumnNames.end_datetime).alias(GoldMeasurementsSAPSeriesColumnNames.end_time),
        F.col(SilverMeasurementsColumnNames.unit).alias(GoldMeasurementsSAPSeriesColumnNames.unit),
        F.col(SilverMeasurementsColumnNames.resolution).alias(GoldMeasurementsSAPSeriesColumnNames.resolution),
        F.current_timestamp().alias(GoldMeasurementsSAPSeriesColumnNames.created),
    )
