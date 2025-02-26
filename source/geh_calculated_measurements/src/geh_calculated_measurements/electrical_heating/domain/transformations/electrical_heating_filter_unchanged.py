from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ColumnNames


def filter_unchanged_electrical_heating(
    newly_calculated_electrical_heating: DataFrame,
    electrical_heating_from_before: DataFrame,
) -> DataFrame:
    return (
        newly_calculated_electrical_heating.alias("current")
        .join(
            electrical_heating_from_before.alias("previous"),
            (
                (
                    F.col(f"current.{ColumnNames.metering_point_id}")
                    == F.col(f"previous.{ColumnNames.metering_point_id}")
                )
                & (F.col(f"current.{ColumnNames.date}") == F.col(f"previous.{ColumnNames.date}"))
                & (F.col(f"current.{ColumnNames.quantity}") == F.col(f"previous.{ColumnNames.quantity}"))
            ),
            "left_anti",
        )
        .select(
            F.col(f"current.{ColumnNames.metering_point_id}"),
            F.col(f"current.{ColumnNames.date}"),
            F.col(f"current.{ColumnNames.quantity}"),
        )
    )
