from typing import Optional

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, TimestampType


def append_if_not_exists(
    spark: SparkSession,
    dataframe: DataFrame,
    table: str,
    merge_columns: list[str],
    clustering_columns_to_filter_specifically: Optional[list[str]] = None,
    target_filters: Optional[dict[str, list[str]]] = None,
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
        f"{current_alias_table_name}.{column_name} <=> {update_alias_table_name}.{column_name}"
        for column_name in merge_columns
    ]

    if target_filters is not None:
        for column_name in target_filters:
            allowed_column_values = ",".join([f"'{x}'" for x in target_filters[column_name]])
            merge_check_list.append(f"{current_alias_table_name}.{column_name} in ({allowed_column_values})")

    if clustering_columns_to_filter_specifically is not None:
        for clustering_key in clustering_columns_to_filter_specifically:
            clustering_key_type = dataframe.schema[clustering_key].dataType
            if isinstance(clustering_key_type, TimestampType) or isinstance(clustering_key_type, DateType):
                extra_merge_check = get_target_filter_for_datetime_clustering_key(
                    dataframe, clustering_key, current_alias_table_name
                )
                merge_check_list.append(extra_merge_check)

    merge_check = " AND ".join(merge_check_list)

    delta_table.alias(current_alias_table_name).merge(
        dataframe.alias(update_alias_table_name), merge_check
    ).whenNotMatchedInsertAll().execute()


def get_target_filter_for_datetime_clustering_key(
    update_df: DataFrame, clustering_col: str, current_alias_table_name: str
) -> str:
    dates_to_filter = [row[0] for row in update_df.select(col(clustering_col).cast("date")).distinct().collect()]
    if len(dates_to_filter) == 0:
        return "TRUE"

    joined_string = ",".join([f"'{str(date)}'" for date in dates_to_filter])
    return f"CAST({current_alias_table_name}.{clustering_col} AS DATE) in ({joined_string})"
