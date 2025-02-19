# TODO AJW: This is a copy of the function from the wholesale codebase.
# This should be moved to a shared location when the time comes.


from pyspark.sql import DataFrame


def write_dataframe_to_table(df: DataFrame, database_name: str, table_name: str, mode: str = "overwrite") -> None:
    df.write.format("delta").mode(mode).saveAsTable(f"{database_name}.{table_name}")
