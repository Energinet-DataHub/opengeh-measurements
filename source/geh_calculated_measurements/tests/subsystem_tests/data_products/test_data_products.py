from pyspark.sql import SparkSession

from geh_calculated_measurements.common.infrastructure import CalculatedMeasurementsDatabaseDefinition
from tests.subsystem_tests.environment_configuration import EnvironmentConfiguration


def test__measurements_calculated__is_streamable(
    spark: SparkSession,
) -> None:
    catalog_name = EnvironmentConfiguration().catalog_name
    database_name = CalculatedMeasurementsDatabaseDefinition.DATABASE_NAME
    view_name = CalculatedMeasurementsDatabaseDefinition.CALCULATED_MEASUREMENTS_VIEW_NAME
    # Set the current catalog to Unity Catalog
    # spark.catalog.setCurrentCatalog("ctl_shres_d_we_001")
    # spark.sql("USE CATALOG ctl_shres_d_we_001") # [PARSE_SYNTAX_ERROR] Syntax error at or near 'ctl_shres_d_we_001': extra input 'ctl_shres_d_we_001'.(line 1, pos 12)

    print("################")
    # print(spark.catalog.currentCatalog())
    # for catalog in spark.catalog.listCatalogs():
    #     print("Catalog: " + catalog.name)
    #     schemas = spark.catalog.listDatabases()
    #     for schema in schemas:
    #         print("    " + schema.name)

    # Read the view as a streaming source using readStream
    streaming_df = spark.readStream.format("delta").table(f"{database_name}.{view_name}")
    query = streaming_df.writeStream.format("memory").queryName("test_query").start()

    try:
        # Wait for the streaming query to initialize
        query.awaitTermination(timeout=5)
        # Assert that the query is active and using the view as a source
        assert query.isActive, "The streaming query is not active"
    finally:
        # Stop the streaming query to clean up resources
        query.stop()
