Feature: Streaming Calculated Measurements to Gold Measurements

  Scenario: Processing calculated measurements to Gold
    Given calculated measurements inserted into the calculated measurements table
    When streaming calculated measurements to the Gold layer
    Then measurements are available in the calculated measurements table
