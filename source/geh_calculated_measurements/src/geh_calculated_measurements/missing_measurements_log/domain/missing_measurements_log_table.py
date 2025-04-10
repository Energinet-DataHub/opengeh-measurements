import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointResolution
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import (
    CalculatedMeasurementsInternalDatabaseDefinition,
    CurrentMeasurementsTable,
    Table,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable


class MissingMeasurementsLogTable(Table):
    def __init__(self, catalog_name: str, time_zone: str) -> None:
        self.catalog_name = catalog_name
        self.time_zone = time_zone
        self.fully_qualified_name = f"{catalog_name}.{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
        super().__init__()

    orchestration_instance_id = T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), False)
    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    date = T.StructField(ContractColumnNames.date, T.DateType(), False)

    def read(self) -> DataFrame:
        current_measurements = CurrentMeasurementsTable(self.catalog_name).read()
        metering_point_periods = MeteringPointPeriodsTable(self.catalog_name).read()

        metering_point_periods_local_time = convert_from_utc(metering_point_periods, self.time_zone)
        expected_daily_entries_local_time = (
            metering_point_periods_local_time.withColumn(
                "start_of_day",
                F.explode(
                    F.sequence(
                        F.col(MeteringPointPeriodsTable.period_from_date),
                        F.col(MeteringPointPeriodsTable.period_to_date),
                        F.expr("INTERVAL 1 DAY"),
                    )
                ),
            )
            .where(
                # to date is exclusive
                F.col("start_of_day") < F.col(MeteringPointPeriodsTable.period_to_date)
            )
            .select(
                F.col(MeteringPointPeriodsTable.metering_point_id),
                F.col("start_of_day"),
                (F.col("start_of_day") + F.expr("INTERVAL 1 DAY")).alias("end_of_day"),
                (
                    (F.unix_timestamp(F.col("end_of_day")) - F.unix_timestamp(F.col("start_of_day")))
                    / F.when(
                        F.col(MeteringPointPeriodsTable.resolution) == MeteringPointResolution.HOUR.value, 3600
                    ).otherwise(900)  # 3600 seconds for 1 hour, 900 seconds for 15 minutes
                ).alias("observations_count"),
            )
        )

        expected_daily_entries = convert_to_utc(expected_daily_entries_local_time, self.time_zone)

        expected_daily_entries.show()

        return expected_daily_entries
