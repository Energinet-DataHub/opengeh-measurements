Feature: Stream Migration to Bronze Migrated

Scenario: Processing full load of migrations to Measurements
  Given the measurements migrated bronze is empty and transactions available in the migration silver table
  When streaming from Migration silver to Measurements
  Then transactions are available in the bronze measurements migration table

Scenario: Processing daily load of migration to measurements
  Given the measurements migrated bronze has data and newer transaction are available in the migration silver table
  When streaming from Migration silver to Measurements
  Then transactions are available in the bronze measurements migration table