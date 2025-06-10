import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from core.gold.domain.constants.column_names.gold_measurements_series_sap_column_names import (
    GoldMeasurementsSeriesSAPColumnNames,
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
            GoldMeasurementsSeriesSAPColumnNames.orchestration_type
        ),
        F.col(SilverMeasurementsColumnNames.metering_point_id).alias(
            GoldMeasurementsSeriesSAPColumnNames.metering_point_id
        ),
        F.col(SilverMeasurementsColumnNames.transaction_id).alias(GoldMeasurementsSeriesSAPColumnNames.transaction_id),
        F.col(SilverMeasurementsColumnNames.transaction_creation_datetime).alias(
            GoldMeasurementsSeriesSAPColumnNames.transaction_creation_datetime
        ),
        F.col(SilverMeasurementsColumnNames.start_datetime).alias(GoldMeasurementsSeriesSAPColumnNames.start_time),
        F.col(SilverMeasurementsColumnNames.end_datetime).alias(GoldMeasurementsSeriesSAPColumnNames.end_time),
        F.col(SilverMeasurementsColumnNames.unit).alias(GoldMeasurementsSeriesSAPColumnNames.unit),
        F.col(SilverMeasurementsColumnNames.resolution).alias(GoldMeasurementsSeriesSAPColumnNames.resolution),
        F.current_timestamp().alias(GoldMeasurementsSeriesSAPColumnNames.created),
    )
