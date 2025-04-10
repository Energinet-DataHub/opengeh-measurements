# TODO BJM: Use from geh_common
class ElectricityMarketMeasurementsInputDatabaseDefinition:
    """This class defines names for the database and tables related to electricity market measurements.

    The database and tables are all defined in Terraform and should be equivalent here for reference.
    """

    DATABASE_NAME = "electricity_market_measurements_input"
    NET_CONSUMPTION_GROUP_6_CHILD_METERING_POINT = "net_consumption_group_6_child_metering_point_v1"
    NET_CONSUMPTION_GROUP_6_CONSUMPTION_METERING_POINT_PERIODS = (
        "net_consumption_group_6_consumption_metering_point_periods_v1"
    )
    MISSING_MEASUREMENTS_LOG_METERING_POINT_PERIODS = "missing_measurements_log_metering_point_periods_v1"
