Feature: Streaming Calculated Measurements to Gold Measurements

  Scenario: Processing calculated measurements to Gold
    Given calculated measurements inserted into the calculated measurements table
    When streaming calculated measurements to the Gold layer
    Then 1 measurements row(s) are available in the gold measurements table

  Scenario: Processing duplicated calculated measurements to Gold
    Given duplicated calculated measurements inserted into the calculated measurements table
    When streaming calculated measurements to the Gold layer
    Then 1 measurements row(s) are available in the gold measurements table

  Scenario: Processing duplicate rows to Gold ensures only one row exists
    Given calculated measurements inserted into the calculated measurements table and the same calculated measurements inserted into the gold table
    When streaming calculated measurements to the Gold layer
    Then 1 measurements row(s) are available in the gold measurements table   