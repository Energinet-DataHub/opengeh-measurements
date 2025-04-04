from core.gold.infrastructure.config.external_view_names import ExternalViewNames
from core.settings.calculated_settings import CalculatedSettings


def create_calculated_measurements_v1_query() -> str:
    calculated_settings = CalculatedSettings()

    return f"""
    CREATE TABLE IF NOT EXISTS {calculated_settings.calculated_database_name}.{ExternalViewNames.calculated_measurements_v1} (
        orchestration_type STRING NOT NULL,
        orchestration_instance_id STRING NOT NULL,
        transaction_id STRING NOT NULL,
        transaction_creation_datetime TIMESTAMP NOT NULL,
        metering_point_id STRING NOT NULL,
        metering_point_type STRING NOT NULL,
        observation_time TIMESTAMP NOT NULL,
        quantity DECIMAL(18,3) NOT NULL,
        quantity_unit STRING NOT NULL,
        quantity_quality STRING NOT NULL,
        resolution STRING NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = true,
        delta.autoOptimize.autoCompact = false,
        delta.deletedFileRetentionDuration = 'interval 30 days'
    );
    """
