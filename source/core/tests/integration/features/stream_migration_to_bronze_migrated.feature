Feature: Stream Migration from Silver to Bronze Measurements

  Scenario: Daily Load: Only newer transactions are migrated from Silver to Bronze
    Given a transaction created 7 days ago and another created 1 days ago in the migration silver table
    When streaming daily load from Migration silver to Measurements Bronze using a cutoff 2 days ago
    Then only the newer transaction should be available in the bronze measurements migration table

  Scenario: Full Load: Migrate Transactions from Silver to Bronze Measurements
    Given transactions available in the migration silver table
    When streaming full load from Migration silver to Measurements Bronze
    Then transactions should be available in the bronze measurements migration table
