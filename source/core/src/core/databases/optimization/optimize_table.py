from databricks.sdk.runtime import display

import core.databases.spark_session as spark_session


def optimize_table(database: str, table: str) -> None:
    """Optimize a table in the database.

    Args:
        database (str): The name of the database.
        table (str): The name of the table to optimize.
    """
    spark = spark_session.initialize_spark()
    display(spark.sql(f"OPTIMIZE {database}.{table}"))
