import pyspark.sql.types as t
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class ChildMeteringPoints(DataFrameWrapper):
    """Represents the child metering points data structure."""

    def __init__(self, df: DataFrame) -> None:
        super().__init__(
            df,
            ChildMeteringPoints.schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    """
    Child metering points related to electrical heating.

    Periods are included when
    - the metering point is of type
      'supply_to_grid' | 'consumption_from_grid' | 'electrical_heating' | 'net_consumption'
    - the metering point is coupled to a parent metering point
      Note: The same child metering point cannot be re-coupled after being uncoupled
    - the child metering point physical status is connected or disconnected.
    - the period does not end before 2021-01-01

    Formatting is according to ADR-144 with the following constraints:
    - No column may use quoted values
    - All date/time values must include seconds
    """
    schema = t.StructType(
        [
            #
            # GSRN number
            t.StructField("metering_point_id", t.StringType(), not nullable),
            #
            # ( 'supply_to_grid' | 'consumption_from_grid' | 'electrical_heating' | 'net_consumption' )
            t.StructField("metering_point_type", t.StringType(), not nullable),
            #
            # ( 'calculated' | 'virtual' | 'physical' )
            t.StructField("metering_point_sub_type", t.StringType(), not nullable),
            #
            # GSRN number
            t.StructField("parent_metering_point_id", t.StringType(), not nullable),
            #
            # The date when the child metering point was coupled to the parent metering point
            # UTC time
            t.StructField("coupled_date", t.TimestampType(), not nullable),
            #
            # The date when the child metering point was uncoupled from the parent metering point
            # UTC time
            t.StructField("uncoupled_date", t.TimestampType(), nullable),
        ]
    )
