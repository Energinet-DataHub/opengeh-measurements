from pyspark.sql import DataFrame


def append_to_table(dataframe: DataFrame, database_name: str, table_name: str) -> None:
    full_table = f"{database_name}.{table_name}"
    dataframe.write.format("delta").mode("append").saveAsTable(full_table)
