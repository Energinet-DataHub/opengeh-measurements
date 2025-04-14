import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames

nullable = True


class Calculations(DataFrameWrapper):
    """The internal storage model of calculations."""

    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=Calculations.schema,
            ignore_nullability=True,
        )

    schema = T.StructType(
        [
            #
            # ID of the orchestration that initiated the calculation job
            T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), not nullable),
            #
            # Calculation year
            T.StructField(ContractColumnNames.calculation_year, T.IntegerType(), not nullable),
            #
            # Calculation month
            T.StructField(ContractColumnNames.calculation_month, T.IntegerType(), not nullable),
            #
            # Execution time of the calculation
            T.StructField(ContractColumnNames.execution_time, T.TimestampType(), not nullable),
        ]
    )
