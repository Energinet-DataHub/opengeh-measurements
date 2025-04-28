from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from geh_calculated_measurements.common.domain import ContractColumnNames


def filter_unchanged_electrical_heating(
    newly_calculated_electrical_heating: DataFrame,
    electrical_heating_from_before: DataFrame,
) -> DataFrame:
    # Changed from left_anti join to left join with filter
    joined_df = (
        newly_calculated_electrical_heating.alias("current")
        .join(
            electrical_heating_from_before.alias("previous"),
            (
                (
                    F.col(f"current.{ContractColumnNames.metering_point_id}")
                    == F.col(f"previous.{ContractColumnNames.metering_point_id}")
                )
                & (F.col(f"current.{ContractColumnNames.date}") == F.col(f"previous.{ContractColumnNames.date}"))
                & (
                    F.col(f"current.{ContractColumnNames.quantity}")
                    == F.col(f"previous.{ContractColumnNames.quantity}")
                )
            ),
            "left",
        )
        .filter(
            # Include rows that either:
            # 1. Don't exist in previous data (unchanged behavior) OR
            # 2. Have is_end_of_period = True (new behavior)
            (F.col(f"previous.{ContractColumnNames.metering_point_id}").isNull())
            | (F.col("current.is_end_of_period") == True)
        )
        .select(
            F.col(f"current.{ContractColumnNames.metering_point_id}"),
            F.col(f"current.{ContractColumnNames.date}"),
            F.col(f"current.{ContractColumnNames.quantity}"),
            F.col("current.is_end_of_period"),
        )
    )

    return joined_df
