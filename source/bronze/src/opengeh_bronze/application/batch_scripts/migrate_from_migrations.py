import opengeh_bronze.application.config.spark_session as spark_session
import opengeh_bronze.infrastructure.batch_scripts.migrate_from_migrations as migrate_from_migrations


def migrate_from_migrations_to_measurements() -> None:
    spark = spark_session.initialize_spark()
    migrate_from_migrations.migrate_from_migrations_to_measurements(spark)
