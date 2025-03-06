class TableNames:
    bronze_submitted_transactions_table = "submitted_transactions"
    bronze_migrated_transactions_table = "migrated_transactions"
    bronze_invalid_submitted_transactions = "invalid_submitted_transactions"
    bronze_submitted_transactions_quarantined = "submitted_transactions_quarantined"


# Table is from the `Migrations` subsystem
class MigrationsTableNames:
    silver_time_series_table = "time_series"
