import pyspark.sql.types as T
from geh_common.pyspark.data_frame_wrapper import DataFrameWrapper
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain.column_names import ContractColumnNames

nullable = True


class Calculations(DataFrameWrapper):
    def __init__(self, df: DataFrame):
        super().__init__(
            df=df,
            schema=calculations_schema,
            ignore_nullability=True,
        )


calculations_schema = T.StructType(
    [
        #
        # ID of the orchestration that initiated the calculation job
        T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), not nullable),
        #
        # Calculation year
        T.StructField("calculation_year", T.IntegerType(), not nullable),
        #
        # Calculation month
        T.StructField("calculation_month", T.IntegerType(), not nullable),
        #
        # Execution time of the calculation
        T.StructField("execution_time", T.TimestampType(), not nullable),
    ]
)
