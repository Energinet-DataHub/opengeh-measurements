Feature: Reading and processing migrated transactions

  Scenario: Reading migrated transactions
    Given migrated transactions are available
    When reading the migrated transactions
    Then the dataframe schema matches the expected migrated transactions schema

  Scenario: Calculating latest migrated timestamp from empty dataset
    Given no migrated transactions are available
    When calculating the latest created timestamp
    Then no timestamp is returned

  Scenario: Calculating latest migrated timestamp from dataset
    Given multiple migrated transactions with different creation dates are available
    When calculating the latest created timestamp
    Then the most recent timestamp is returned
