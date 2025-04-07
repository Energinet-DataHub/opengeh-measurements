Feature: Streaming Calculated Measurements to Gold

  Scenario: Processing Calculated Measurements To Gold
    Given new valid calculated measurements
    When streaming the calculated measurements
    Then it is available in the Gold table
