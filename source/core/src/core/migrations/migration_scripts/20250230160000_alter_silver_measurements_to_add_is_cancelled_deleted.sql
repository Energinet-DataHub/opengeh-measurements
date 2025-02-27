ALTER TABLE {silver_database}.{silver_measurements_table}
ADD COLUMNS (is_cancelled boolean after points, is_deleted boolean after is_cancelled)
