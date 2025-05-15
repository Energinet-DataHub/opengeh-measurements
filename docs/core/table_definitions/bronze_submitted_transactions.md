# Measurements - Bronze Submitted Transactions Table Definitions

This tables contains submitted transactions from received from the ProcessManager through an eventhub.

| Column Name | Data type | Nullable | Description | Constraints |
| ----------- | --------- | -------- | ----------- | ----------- |
| key | BinaryType | True | | |
| value | BinaryType | True | Contains the value from the eventhub message. Expecting a transaction. | |
| topic | StringType | True | EventHub topic | |
| partition | IntegerType | True | | |
| offset | LongType | True | | |
| timestamp | TimestampType | True | | |
| timestamp_type | IntegerType | True | | |
