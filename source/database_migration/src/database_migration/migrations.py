from spark_sql_migrations import (
    migration_pipeline,
    SparkSqlMigrationsConfiguration,
    create_and_configure_container
)


def migrate():
    _configure_spark_sql_migrations()
    migration_pipeline.migrate()


def _configure_spark_sql_migrations():
    spark_config = SparkSqlMigrationsConfiguration(
        migration_schema_name="test",
        migration_table_name="migrations",
        migration_scripts_folder_path="database_migration.migration_scripts",
        substitution_variables={},
        catalog_name="catalog"
    )

    create_and_configure_container(spark_config)
