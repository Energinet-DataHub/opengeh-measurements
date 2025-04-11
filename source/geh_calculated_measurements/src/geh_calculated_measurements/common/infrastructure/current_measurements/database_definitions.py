from geh_common.data_products.measurements_core.measurements_gold import current_v1


class MeasurementsGoldDatabaseDefinition:
    DATABASE_NAME = current_v1.database_name
    CURRENT_MEASUREMENTS = current_v1.view_name
