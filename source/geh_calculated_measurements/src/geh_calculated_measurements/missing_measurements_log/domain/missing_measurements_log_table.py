from uuid import UUID

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
    def __init__(self, catalog_name: str, time_zone: str, orchestration_instance_id: UUID) -> None:
        self.catalog_name = catalog_name
        self.time_zone = time_zone
        self.input_orchestration_instance_id = orchestration_instance_id
        self.fully_qualified_name = f"{catalog_name}.{CalculatedMeasurementsInternalDatabaseDefinition.DATABASE_NAME}.{CalculatedMeasurementsInternalDatabaseDefinition.MISSING_MEASUREMENTS_LOG_TABLE_NAME}"
        super().__init__()

    orchestration_instance_id = T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), False)
    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    date = T.StructField(ContractColumnNames.date, T.TimestampType(), False)

    def _get_expected_measurement_counts(self, metering_point_periods: DataFrame) -> DataFrame:
        metering_point_periods_local_time = convert_from_utc(metering_point_periods, self.time_zone)
        expected_measurement_counts = (
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
                    ).when(F.col(MeteringPointPeriodsTable.resolution) == MeteringPointResolution.QUARTER.value, 900)
                    # 3600 seconds for 1 hour, 900 seconds for 15 minutes
                ).alias("measurement_counts"),
            )
            .select(
                F.col(MeteringPointPeriodsTable.metering_point_id),
                F.col("start_of_day").alias(self.date),
                F.col("measurement_counts"),
            )
        )

        return convert_to_utc(expected_measurement_counts, self.time_zone)

    def _get_actual_measurement_counts(self, current_measurements: DataFrame) -> DataFrame:
        # Filter quality=missing
        current_measurements = convert_from_utc(current_measurements, self.time_zone)
        actual_measurement_counts = (
            current_measurements.withColumn(self.date, F.to_date(F.col(CurrentMeasurementsTable.observation_time)))
            .groupBy(self.date, CurrentMeasurementsTable.metering_point_id)
            .agg(F.count("*").alias("measurement_counts"))
        ).select(
            F.col(CurrentMeasurementsTable.metering_point_id),
            F.col(self.date),
            F.col("measurement_counts"),
        )

        return actual_measurement_counts

    # TODO JMG: use_span?
    def read(self) -> DataFrame:
        current_measurements = CurrentMeasurementsTable(self.catalog_name).read()
        metering_point_periods = MeteringPointPeriodsTable(self.catalog_name).read()

        expected_measurement_counts = self._get_expected_measurement_counts(metering_point_periods)
        actual_measurement_counts = self._get_actual_measurement_counts(current_measurements)

        # Find missing measurements
        missing_measurements = expected_measurement_counts.join(
            actual_measurement_counts,
            [
                expected_measurement_counts[MeteringPointPeriodsTable.metering_point_id]
                == actual_measurement_counts[CurrentMeasurementsTable.metering_point_id],
                expected_measurement_counts[self.date] == actual_measurement_counts[self.date],
                expected_measurement_counts["measurement_counts"] == actual_measurement_counts["measurement_counts"],
            ],
            "left_anti",
        ).select(
            F.lit(str(self.input_orchestration_instance_id)).alias(self.orchestration_instance_id),
            F.col(self.metering_point_id),
            F.col(self.date),
        )

        return missing_measurements
