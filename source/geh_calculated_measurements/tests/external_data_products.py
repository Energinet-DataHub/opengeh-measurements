from dataclasses import dataclass
from typing import Any

from geh_common.data_products.electricity_market_measurements_input import (
    capacity_settlement_metering_point_periods_v1,
    electrical_heating_child_metering_points_v1,
    electrical_heating_consumption_metering_point_periods_v1,
    missing_measurements_log_metering_point_periods_v1,
    net_consumption_group_6_child_metering_points_v1,
    net_consumption_group_6_consumption_metering_point_periods_v1,
)
from geh_common.data_products.measurements_core.measurements_gold import current_v1 as current


@dataclass
class DataProduct:
    database_name: str
    view_name: str
    schema: Any


class ExternalDataProducts:
    """
    This class contains all external data products used in the project.

    The intention is to have all data product dependencies defined in one place in the test code. This way, it is easier to bump for instance the version of a data product.
    """

    CURRENT_MEASUREMENTS: DataProduct = DataProduct(
        database_name=current.database_name,
        view_name=current.view_name,
        schema=current.schema,
    )

    CAPACITY_SETTLEMENT_METERING_POINT_PERIODS: DataProduct = DataProduct(
        database_name=capacity_settlement_metering_point_periods_v1.database_name,
        view_name=capacity_settlement_metering_point_periods_v1.view_name,
        schema=capacity_settlement_metering_point_periods_v1.schema,
    )

    ELECTRICAL_HEATING_CHILD_METERING_POINTS: DataProduct = DataProduct(
        database_name=electrical_heating_child_metering_points_v1.database_name,
        view_name=electrical_heating_child_metering_points_v1.view_name,
        schema=electrical_heating_child_metering_points_v1.schema,
    )

    ELECTRICAL_HEATING_CONSUMPTION_METERING_POINT_PERIODS: DataProduct = DataProduct(
        database_name=electrical_heating_consumption_metering_point_periods_v1.database_name,
        view_name=electrical_heating_consumption_metering_point_periods_v1.view_name,
        schema=electrical_heating_consumption_metering_point_periods_v1.schema,
    )

    NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINTS: DataProduct = DataProduct(
        database_name=net_consumption_group_6_child_metering_points_v1.database_name,
        view_name=net_consumption_group_6_child_metering_points_v1.view_name,
        schema=net_consumption_group_6_child_metering_points_v1.schema,
    )

    NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS: DataProduct = DataProduct(
        database_name=net_consumption_group_6_consumption_metering_point_periods_v1.database_name,
        view_name=net_consumption_group_6_consumption_metering_point_periods_v1.view_name,
        schema=net_consumption_group_6_consumption_metering_point_periods_v1.schema,
    )

    MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS: DataProduct = DataProduct(
        database_name=missing_measurements_log_metering_point_periods_v1.database_name,
        view_name=missing_measurements_log_metering_point_periods_v1.view_name,
        schema=missing_measurements_log_metering_point_periods_v1.schema,
    )

    @staticmethod
    def get_all_database_names() -> list[str]:
        return list(
            set(
                getattr(getattr(ExternalDataProducts, attr), "database_name")
                for attr in dir(ExternalDataProducts)
                if isinstance(getattr(ExternalDataProducts, attr), DataProduct)
            )
        )

    @staticmethod
    def get_all_data_products() -> list[DataProduct]:
        return [
            getattr(ExternalDataProducts, attr)
            for attr in dir(ExternalDataProducts)
            if isinstance(getattr(ExternalDataProducts, attr), DataProduct)
        ]
