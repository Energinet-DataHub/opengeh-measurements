Feature: Streaming from Silver and Bronze to Gold

  Scenario: Processing calculated measurements to Gold
    Given calculated measurements inserted into the calculated measurements table
    When streaming calculated measurements to the Gold layer
    Then measurements are available in the calculated measurements table

  Scenario: Processing Silver measurements to Gold
    Given valid measurements inserted into the silver measurements table
    When streaming Silver measurements to the Gold layer
    Then measurements are available in the gold measurements table

  Scenario: Creating receipt entry during Silver to Gold streaming
    Given valid measurements inserted into the silver measurements table
    When streaming Silver measurements to the Gold layer
    Then a receipt entry is available in the process manager receipts table

  Scenario: Migrated transactions from Bronze to Gold
    Given valid migrated transactions inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then valid migrated transactions are available in the gold measurements table

  Scenario: Filtering migrated transactions before 2017
    Given migrated transactions dated before 2017 inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then no measurements are available in the gold measurements table