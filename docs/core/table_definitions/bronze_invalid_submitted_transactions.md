# Measurements - Bronze Invalid Submitted Transactions Table Definitions

This table contains invalid submitted transactions received from the ProcessManager through an eventhub.

In this case, invalid means that we could not unpack the `value` column. This can be because we don't recognize the `version` or the value in `value` does not match the protobuf contract.

| Column Name | Data type | Nullable | Description | Constraints |
| ----------- | --------- | -------- | ----------- | ----------- |
| key | BinaryType | True | | |
| value | BinaryType | True | Contains the value from the eventhub message. Expecting a transaction. | |
| topic | StringType | True | EventHub topic | |
| partition | IntegerType | True | | |
| offset | LongType | True | | |
| timestamp | TimestampType | True | | |
| timestamp_type | IntegerType | True | | |
| version | StringType | True | Extracted version from `value`. If None, then the version could not be extracted. We expect `version` to be the first property. | |
