# Table Update Strategy

## Append

This strategy is about appending to a delta table.

## Append If Not Exists

This strategy is about appending to a delta table, unless the rows already exists.

## Table Operations

| Layer | Delta Table | Method |
| - | - | - |
| **Bronze** | | |
| | Submitted Transactions | Append |
| | Invalid Submitted Transactions | Append |
| | Migrated Transactions | Append |
| | Submitted Transactions Quarantined | Append |
| **Silver** | | |
| | Measurements | Append If Not Exists |
| **Gold** | | |
| | Measurements | Append If Not Exists |
