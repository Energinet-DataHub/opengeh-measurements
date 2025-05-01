Feature: Streaming Migrated Bronze Measurements to Gold Measurements

  Scenario: Migrated transactions from Bronze to Gold
    Given valid migrated transactions inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then 24 migrated measurements rows are available in the gold measurements table

  Scenario: Filtering migrated transactions before 2017
    Given migrated transactions dated before 2017 inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then no measurements are available in the gold measurements table

  Scenario: Processing duplicated migrated transactions from Bronze to Gold
    Given duplicated valid migrated transactions inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then 24 migrated measurements rows are available in the gold measurements table

  Scenario: Processing duplicate rows to Gold ensures unique rows exists
    Given valid migrated transaction inserted into the bronze migratied transactions table and the same transaction inserted into the gold table
    When streaming migrated transactions to the Gold layer
    Then 24 migrated measurements rows are available in the gold measurements table        
