from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from pyspark.sql import DataFrame

import core.silver.infrastructure.config.spark_session as spark_session
from core.gold.domain.constants.column_names.gold_measurements_sap_series_column_names import (
    GoldMeasurementsSAPSeriesColumnNames,
)
from core.gold.domain.constants.streaming.query_names import QueryNames
from core.gold.infrastructure.config import GoldTableNames
from core.settings.gold_settings import GoldSettings
from core.utility import delta_table_helper


class GoldMeasurementsSAPSeriesRepository:
    def __init__(self) -> None:
        database_name = GoldSettings().gold_database_name
        self.table = f"{database_name}.{GoldTableNames.gold_measurements_sap_series}"
        self.spark = spark_session.initialize_spark()

    def append_if_not_exists(self, silver_measurements: DataFrame, query_name: QueryNames) -> None:
        """Append to the table unless there are duplicates based on all columns except 'created'.

        :param silver_measurements: DataFrame containing the data to be appended.
        """
        orchestration_type_filters = self._get_possible_orchestration_types_for_stream(query_name)

        delta_table_helper.append_if_not_exists(
            self.spark,
            silver_measurements,
            self.table,
            self._merge_columns(),
            target_filters=orchestration_type_filters,
        )

    def _merge_columns(self) -> list[str]:
        return [
            GoldMeasurementsSAPSeriesColumnNames.orchestration_type,
            GoldMeasurementsSAPSeriesColumnNames.metering_point_id,
            GoldMeasurementsSAPSeriesColumnNames.transaction_id,
            GoldMeasurementsSAPSeriesColumnNames.transaction_creation_datetime,
            GoldMeasurementsSAPSeriesColumnNames.start_time,
            GoldMeasurementsSAPSeriesColumnNames.resolution,
        ]

    # We need to filter on specific orchestration types per stream, to avoid overlapping writes crashing streams.
    def _get_possible_orchestration_types_for_stream(
        self,
        query_name: QueryNames,
    ) -> dict[str, list[str]]:
        if query_name == QueryNames.SILVER_TO_GOLD_SAP_SERIES:
            return {
                GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [GehCommonOrchestrationType.SUBMITTED.value]
            }
        if query_name == QueryNames.MIGRATIONS_TO_SAP_SERIES_GOLD:
            return {
                GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [GehCommonOrchestrationType.MIGRATION.value]
            }
        if query_name == QueryNames.CALCULATED_TO_GOLD_SAP_SERIES:
            return {
                GoldMeasurementsSAPSeriesColumnNames.orchestration_type: [
                    GehCommonOrchestrationType.CAPACITY_SETTLEMENT.value,
                    GehCommonOrchestrationType.ELECTRICAL_HEATING.value,
                    GehCommonOrchestrationType.NET_CONSUMPTION.value,
                ]
            }
        return {}
