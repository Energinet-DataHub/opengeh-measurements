# Measurements Internal Core - Process Manager Receipts

Thie table contains every `orchestration_instance_id` that have been received from the ProcessManager in the bronze submitted transactions, that have now been transformed to the gold measurements table.
This means that we have to notify the Process Manager, that we have processed the `orchestration_instance_id`.

| Column Name | Data type | Nullable | Description | Constraints |
| ----------- | --------- | -------- | ----------- | ----------- |
| orchestration_instance_id | StringType | True | | |
| created | TimestampType | True | | |
