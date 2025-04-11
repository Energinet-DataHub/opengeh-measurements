from uuid import UUID

import pyspark.sql.functions as F
import pyspark.sql.types as T
from geh_common.domain.types import MeteringPointResolution
from geh_common.pyspark.transformations import convert_from_utc, convert_to_utc
from pyspark.sql import DataFrame

from geh_calculated_measurements.common.domain import ContractColumnNames
from geh_calculated_measurements.common.infrastructure import (
    CurrentMeasurementsTable,
    Table,
)
from geh_calculated_measurements.missing_measurements_log.infrastructure import MeteringPointPeriodsTable
from geh_common.data_products.measurements_calculated import missing_measurements_log_v1


class MissingMeasurementsLogTable(Table):
    def __init__(self, catalog_name: str, time_zone: str, orchestration_instance_id: UUID) -> None:
        self.catalog_name = catalog_name
        self.time_zone = time_zone
        self.input_orchestration_instance_id = orchestration_instance_id
        self.fully_qualified_name = f"{catalog_name}.{missing_measurements_log_v1.database_name}.{missing_measurements_log_v1.vuew_name"
        super().__init__()

    orchestration_instance_id = T.StructField(ContractColumnNames.orchestration_instance_id, T.StringType(), False)
    metering_point_id = T.StructField(ContractColumnNames.metering_point_id, T.StringType(), False)
    date = T.StructField(ContractColumnNames.date, T.TimestampType(), False)

    def _get_expected_measurement_counts(self, metering_point_periods: DataFrame) -> DataFrame:
        """Calculate the expected measurement counts grouped by metering point and date."""
        metering_point_periods_local_time = convert_from_utc(metering_point_periods, self.time_zone)
        expected_measurement_counts = (
            metering_point_periods_local_time.withColumn(
                "start_of_day",
                F.explode(
                    F.sequence(
                        F.col(MeteringPointPeriodsTable.period_from_date.name),
                        F.col(MeteringPointPeriodsTable.period_to_date.name),
                        F.expr("INTERVAL 1 DAY"),
                    )
                ),
            )
            .where(
                # to date is exclusive
                F.col("start_of_day") < F.col(MeteringPointPeriodsTable.period_to_date.name)
            )
            .select(
                F.col(MeteringPointPeriodsTable.metering_point_id.name),
                F.col("start_of_day"),
                (F.col("start_of_day") + F.expr("INTERVAL 1 DAY")).alias("end_of_day"),
                (
                    (F.unix_timestamp(F.col("end_of_day")) - F.unix_timestamp(F.col("start_of_day")))
                    / F.when(
                        F.col(MeteringPointPeriodsTable.resolution.name) == MeteringPointResolution.HOUR.value, 3600
                    ).when(
                        F.col(MeteringPointPeriodsTable.resolution.name) == MeteringPointResolution.QUARTER.value, 900
                    )
                    # 3600 seconds for 1 hour, 900 seconds for 15 minutes
                ).alias("measurement_counts"),
            )
            .select(
                F.col(MeteringPointPeriodsTable.metering_point_id.name),
                F.col("start_of_day").alias(self.date.name),
                F.col("measurement_counts"),
            )
        )

        return convert_to_utc(expected_measurement_counts, self.time_zone)

    def _get_actual_measurement_counts(self, current_measurements: DataFrame) -> DataFrame:
        """Calculate the actual measurement counts grouped by metering point and date."""
        current_measurements = current_measurements.where(
            F.col(CurrentMeasurementsTable.observation_time.name).isNotNull()
        )

        current_measurements = convert_from_utc(current_measurements, self.time_zone)
        actual_measurement_counts = (
            current_measurements.withColumn(
                self.date.name, F.to_date(F.col(CurrentMeasurementsTable.observation_time.name))
            )
            .groupBy(self.date.name, CurrentMeasurementsTable.metering_point_id.name)
            .agg(F.count("*").alias("measurement_counts"))
        ).select(
            F.col(CurrentMeasurementsTable.metering_point_id.name),
            F.col(self.date.name),
            F.col("measurement_counts"),
        )

        return convert_to_utc(actual_measurement_counts, self.time_zone)

    def _get_missing_measurements(
        self, expected_measurement_counts: DataFrame, actual_measurement_counts: DataFrame
    ) -> DataFrame:
        return expected_measurement_counts.join(
            actual_measurement_counts,
            [
                expected_measurement_counts[MeteringPointPeriodsTable.metering_point_id.name]
                == actual_measurement_counts[CurrentMeasurementsTable.metering_point_id.name],
                expected_measurement_counts[self.date.name] == actual_measurement_counts[self.date.name],
                expected_measurement_counts["measurement_counts"] == actual_measurement_counts["measurement_counts"],
            ],
            "left_anti",
        ).select(
            F.lit(str(self.input_orchestration_instance_id)).alias(self.orchestration_instance_id.name),
            F.col(self.metering_point_id.name),
            F.col(self.date.name),
        )

    def read(self) -> DataFrame:
        current_measurements = CurrentMeasurementsTable(self.catalog_name).read()
        metering_point_periods = MeteringPointPeriodsTable(self.catalog_name).read()

        expected_measurement_counts = self._get_expected_measurement_counts(metering_point_periods)
        actual_measurement_counts = self._get_actual_measurement_counts(current_measurements)

        return self._get_missing_measurements(
            expected_measurement_counts=expected_measurement_counts,
            actual_measurement_counts=actual_measurement_counts,
        )
