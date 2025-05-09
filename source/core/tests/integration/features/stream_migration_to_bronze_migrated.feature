Feature: Stream Migration from Silver to Bronze Measurements

  Scenario: Daily Load: Migrate Transactions from Silver to Bronze Measurements
    Given transactions available in the migration silver table
    When streaming daily load from Migration silver to Measurements Bronze
    Then transactions should be available in the bronze measurements migration table

  Scenario: Full Load: Migrate Transactions from Silver to Bronze Measurements
    Given transactions available in the migration silver table
    When streaming full load from Migration silver to Measurements Bronze
    Then transactions should be available in the bronze measurements migration table