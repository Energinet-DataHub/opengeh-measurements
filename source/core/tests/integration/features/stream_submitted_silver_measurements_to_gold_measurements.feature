Feature: Streaming Submitted Silver Measurements to Gold Measurements

  Scenario: Processing Silver measurements to Gold
    Given valid measurements inserted into the silver measurements table
    When streaming Silver measurements to the Gold layer
    Then 24 measurements rows are available in the gold measurements table

  Scenario: Creating receipt entry during Silver to Gold streaming
    Given valid measurements with an orchestration instance id inserted into the silver measurements table
    When streaming Silver measurements to the Gold layer
    Then a receipt entry is available in the process manager receipts table

  Scenario: Processing duplicated Silver measurements to Gold
    Given duplicated valid measurements inserted into the silver measurements table
    When streaming Silver measurements to the Gold layer
    Then 24 measurements rows are available in the gold measurements table

  Scenario: Processing duplicate rows to Gold ensures unique rows exists
    Given valid measurements inserted into the silver measurements table and the same calculated measurements inserted into the gold table
    When streaming Silver measurements to the Gold layer
    Then 24 measurements rows are available in the gold measurements table