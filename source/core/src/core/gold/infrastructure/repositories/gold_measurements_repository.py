from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.gold.infrastructure.config.spark as spark_session
import core.utility.delta_table_helper as delta_table_helper
from core.gold.domain.constants.column_names.gold_measurements_column_names import GoldMeasurementsColumnNames
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings


class GoldMeasurementsRepository:
    def __init__(self) -> None:
        self.gold_database_name = GoldSettings().gold_database_name
        self.table = f"{self.gold_database_name}.{GoldTableNames.gold_measurements}"
        self.spark = spark_session.initialize_spark()

    def append_if_not_exists(self, gold_measurements: DataFrame, query_name: QueryNames) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param gold_measurements: DataFrame containing the data to be appended.
        """
        orchestration_type_filters = self._get_possible_orchestration_types_for_stream(query_name)
        clustering_keys_to_filter = [
            GoldMeasurementsColumnNames.transaction_creation_datetime,
            GoldMeasurementsColumnNames.observation_time,
        ]

        delta_table_helper.append_if_not_exists(
            self.spark,
            gold_measurements,
            self.table,
            self._merge_columns(query_name),
            clustering_columns_to_filter_specifically=clustering_keys_to_filter,
            target_filters=orchestration_type_filters,
        )

    def _merge_columns(self, query_name: QueryNames) -> list[str]:
        if query_name == QueryNames.MIGRATIONS_TO_GOLD:
            return [
                GoldMeasurementsColumnNames.metering_point_id,
                GoldMeasurementsColumnNames.observation_time,
                GoldMeasurementsColumnNames.transaction_creation_datetime,
                GoldMeasurementsColumnNames.transaction_id,
                GoldMeasurementsColumnNames.resolution,
                GoldMeasurementsColumnNames.is_cancelled,
            ]

        return [
            GoldMeasurementsColumnNames.metering_point_id,
            GoldMeasurementsColumnNames.orchestration_type,
            GoldMeasurementsColumnNames.observation_time,
            GoldMeasurementsColumnNames.transaction_id,
            GoldMeasurementsColumnNames.transaction_creation_datetime,
        ]

    # We need to filter on specific orchestration types per stream, to avoid overlapping writes crashing streams.
    def _get_possible_orchestration_types_for_stream(
        self,
        query_name: QueryNames,
    ) -> dict[str, list[str]]:
        if query_name == QueryNames.SILVER_TO_GOLD:
            return {GoldMeasurementsColumnNames.orchestration_type: [GehCommonOrchestrationType.SUBMITTED.value]}
        if query_name == QueryNames.MIGRATIONS_TO_GOLD:
            return {GoldMeasurementsColumnNames.orchestration_type: [GehCommonOrchestrationType.MIGRATION.value]}
        if query_name == QueryNames.CALCULATED_TO_GOLD:
            return {
                GoldMeasurementsColumnNames.orchestration_type: [
                    GehCommonOrchestrationType.CAPACITY_SETTLEMENT.value,
                    GehCommonOrchestrationType.ELECTRICAL_HEATING.value,
                    GehCommonOrchestrationType.NET_CONSUMPTION.value,
                ]
            }
        return {}
