import pyspark.sql.types as t
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

nullable = True


class TimeSeriesPoints(DataFrameWrapper):
    """Represents the time series points data structure."""

    def __init__(self, df: DataFrame) -> None:
        super().__init__(
            df,
            schema=TimeSeriesPoints.schema,
            # We ignore_nullability because it has turned out to be too hard and even possibly
            # introducing more errors than solving in order to stay in exact sync with the
            # logically correct schema.
            ignore_nullability=True,
        )

    # Time series points related to electrical heating.
    #
    # Points are included when:
    # - the unit is kWh
    # - the metering point type is one of those listed below
    # - the observation time is after 2021-01-01
    schema = t.StructType(
        [
            #
            # GSRN number
            t.StructField("metering_point_id", t.StringType(), not nullable),
            #
            # 'consumption' | 'supply_to_grid' | 'consumption_from_grid' |
            # 'electrical_heating' | 'net_consumption'
            t.StructField("metering_point_type", t.StringType(), not nullable),
            #
            # UTC time
            t.StructField(
                "observation_time",
                t.TimestampType(),
                not nullable,
            ),
            #
            t.StructField("quantity", t.DecimalType(18, 3), not nullable),
        ]
    )
