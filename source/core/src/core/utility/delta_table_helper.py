from typing import Optional

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def append_if_not_exists(
    spark: SparkSession,
    dataframe: DataFrame,
    table: str,
    merge_columns: list[str],
    target_filters: Optional[dict[str, str]] = None,
) -> None:
    """Append to table unless there are duplicates based on merge columns.

    :spark: SparkSession
    :param dataframe: DataFrame containing the data to be appended.
    :param table: The table to append to.
    :param merge_columns: List of column names to merge on.
    :param target_filters: Dict of column names to filter values, applied to the target table during the merge.
    """
    delta_table = DeltaTable.forName(spark, table)
    current_alias_table_name = "current"
    update_alias_table_name = "update"

    dataframe = dataframe.dropDuplicates(subset=merge_columns)

    merge_check_list = [
        f"{current_alias_table_name}.{column_name} IS NOT DISTINCT FROM {update_alias_table_name}.{column_name}"
        for column_name in merge_columns
    ]

    if target_filters is not None:
        target_filter_list = [
            f"{current_alias_table_name}.{column_name} <=> {target_filters[column_name]}"
            for column_name in target_filters
        ]
        merge_check_list += target_filter_list

    merge_check = " AND ".join(merge_check_list)

    delta_table.alias(current_alias_table_name).merge(
        dataframe.alias(update_alias_table_name), merge_check
    ).whenNotMatchedInsertAll().execute()
