import pyspark.sql.functions as F
from geh_common.domain.types.metering_point_resolution import MeteringPointResolution as GehCommonResolution
from geh_common.domain.types.metering_point_type import MeteringPointType as GehCommonMeteringPointType
from geh_common.domain.types.orchestration_type import OrchestrationType as GehCommonOrchestrationType
from geh_common.domain.types.quantity_unit import QuantityUnit as GehCommonUnit
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import DecimalType

import core.silver.infrastructure.config.spark_session as spark_session
import core.utility.datetime_helper as datetime_helper
from core.bronze.domain.constants.column_names.bronze_submitted_transactions_column_names import (
    ValueColumnNames,
)
from core.contracts.process_manager.enums.metering_point_type import MeteringPointType as CoreMeteringPointType
from core.contracts.process_manager.enums.orchestration_type import OrchestrationType as CoreOrchestrationType
from core.contracts.process_manager.enums.resolution import Resolution as CoreResolution
from core.contracts.process_manager.enums.unit import Unit as CoreUnit
from core.silver.domain.constants.column_names.silver_measurements_column_names import SilverMeasurementsColumnNames


def transform(unpacked_submitted_transactions: DataFrame) -> DataFrame:
    spark = spark_session.initialize_spark()
    current_utc_time = datetime_helper.get_current_utc_timestamp(spark)

    measurements = unpacked_submitted_transactions.select(
        _align_orchestration_type().alias(SilverMeasurementsColumnNames.orchestration_type),
        unpacked_submitted_transactions[ValueColumnNames.orchestration_instance_id].alias(
            SilverMeasurementsColumnNames.orchestration_instance_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.metering_point_id].alias(
            SilverMeasurementsColumnNames.metering_point_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_id].alias(
            SilverMeasurementsColumnNames.transaction_id
        ),
        unpacked_submitted_transactions[ValueColumnNames.transaction_creation_datetime].alias(
            SilverMeasurementsColumnNames.transaction_creation_datetime
        ),
        _align_metering_point_type().alias(SilverMeasurementsColumnNames.metering_point_type),
        _align_unit().alias(SilverMeasurementsColumnNames.unit),
        _align_resolution().alias(SilverMeasurementsColumnNames.resolution),
        unpacked_submitted_transactions[ValueColumnNames.start_datetime].alias(
            SilverMeasurementsColumnNames.start_datetime
        ),
        unpacked_submitted_transactions[ValueColumnNames.end_datetime].alias(
            SilverMeasurementsColumnNames.end_datetime
        ),
        F.transform(
            ValueColumnNames.points,
            lambda x: F.struct(
                x.position.alias(SilverMeasurementsColumnNames.Points.position),
                (x.quantity.units + (x.quantity.nanos / 1_000_000_000))
                .cast(DecimalType(18, 3))
                .alias(SilverMeasurementsColumnNames.Points.quantity),
                x.quality.alias(SilverMeasurementsColumnNames.Points.quality),
            ),
        ).alias(SilverMeasurementsColumnNames.points),
        F.lit(False).alias(SilverMeasurementsColumnNames.is_cancelled),
        F.lit(False).alias(SilverMeasurementsColumnNames.is_deleted),
        current_utc_time.alias(SilverMeasurementsColumnNames.created),
    )

    return measurements


def _align_orchestration_type() -> Column:
    return F.when(
        F.col(SilverMeasurementsColumnNames.orchestration_type)
        == CoreOrchestrationType.OT_SUBMITTED_MEASURE_DATA.value,
        GehCommonOrchestrationType.SUBMITTED.value,
    ).otherwise(F.col(SilverMeasurementsColumnNames.orchestration_type))


def _align_metering_point_type() -> Column:
    return (
        F.when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_CONSUMPTION.value,
            GehCommonMeteringPointType.CONSUMPTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_PRODUCTION.value,
            GehCommonMeteringPointType.PRODUCTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_EXCHANGE.value,
            GehCommonMeteringPointType.EXCHANGE.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_VE_PRODUCTION.value,
            GehCommonMeteringPointType.VE_PRODUCTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_ANALYSIS.value,
            GehCommonMeteringPointType.ANALYSIS.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_NOT_USED.value,
            GehCommonMeteringPointType.NOT_USED.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_SURPLUS_PRODUCTION_GROUP_6.value,
            GehCommonMeteringPointType.SURPLUS_PRODUCTION_GROUP_6.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_NET_PRODUCTION.value,
            GehCommonMeteringPointType.NET_PRODUCTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_SUPPLY_TO_GRID.value,
            GehCommonMeteringPointType.SUPPLY_TO_GRID.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_CONSUMPTION_FROM_GRID.value,
            GehCommonMeteringPointType.CONSUMPTION_FROM_GRID.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_WHOLESALE_SERVICES_INFORMATION.value,
            GehCommonMeteringPointType.WHOLESALE_SERVICES_INFORMATION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_OWN_PRODUCTION.value,
            GehCommonMeteringPointType.OWN_PRODUCTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_NET_FROM_GRID.value,
            GehCommonMeteringPointType.NET_FROM_GRID.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_NET_TO_GRID.value,
            GehCommonMeteringPointType.NET_TO_GRID.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_TOTAL_CONSUMPTION.value,
            GehCommonMeteringPointType.TOTAL_CONSUMPTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_NET_LOSS_CORRECTION.value,
            GehCommonMeteringPointType.NET_LOSS_CORRECTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_ELECTRICAL_HEATING.value,
            GehCommonMeteringPointType.ELECTRICAL_HEATING.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_NET_CONSUMPTION.value,
            GehCommonMeteringPointType.NET_CONSUMPTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_OTHER_CONSUMPTION.value,
            GehCommonMeteringPointType.OTHER_CONSUMPTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_OTHER_PRODUCTION.value,
            GehCommonMeteringPointType.OTHER_PRODUCTION.value,
        )
        # TODO: Contact Mosaic, and make them send MPT_EFFECT_PAYMENT as MPT_CAPACITY_SETTLEMENT
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type) == CoreMeteringPointType.MPT_EFFECT_PAYMENT.value,
            GehCommonMeteringPointType.CAPACITY_SETTLEMENT.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_EXCHANGE_REACTIVE_ENERGY.value,
            GehCommonMeteringPointType.EXCHANGE_REACTIVE_ENERGY.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_COLLECTIVE_NET_PRODUCTION.value,
            GehCommonMeteringPointType.COLLECTIVE_NET_PRODUCTION.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.metering_point_type)
            == CoreMeteringPointType.MPT_COLLECTIVE_NET_CONSUMPTION.value,
            GehCommonMeteringPointType.COLLECTIVE_NET_CONSUMPTION.value,
        )
        .otherwise(F.col(SilverMeasurementsColumnNames.metering_point_type))
    )


def _align_unit() -> Column:
    return (
        F.when(F.col(SilverMeasurementsColumnNames.unit) == CoreUnit.U_KWH.value, GehCommonUnit.KWH.value)
        .when(F.col(SilverMeasurementsColumnNames.unit) == CoreUnit.U_KW.value, GehCommonUnit.KW.value)
        .when(F.col(SilverMeasurementsColumnNames.unit) == CoreUnit.U_MWH.value, GehCommonUnit.MWH.value)
        .when(F.col(SilverMeasurementsColumnNames.unit) == CoreUnit.U_TONNE.value, GehCommonUnit.TONNE.value)
        .when(F.col(SilverMeasurementsColumnNames.unit) == CoreUnit.U_KVARH.value, GehCommonUnit.KVARH.value)
        .otherwise(F.col(SilverMeasurementsColumnNames.unit))
    )


def _align_resolution() -> Column:
    return (
        F.when(
            F.col(SilverMeasurementsColumnNames.resolution) == CoreResolution.R_PT15M.value,
            GehCommonResolution.QUARTER.value,
        )
        .when(
            F.col(SilverMeasurementsColumnNames.resolution) == CoreResolution.R_PT1H.value,
            GehCommonResolution.HOUR.value,
        )
        .otherwise(F.col(SilverMeasurementsColumnNames.resolution))
    )
