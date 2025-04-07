Feature: Streaming Silver Measurements to Gold

  Scenario: Processing Silver Measurements To Gold
    Given hourly valid silver measurements for one day
    When streaming to Gold
    Then there will be 24 points available in the Gold Table

  Scenario: Processing Silver Measurements To Gold
    Given quarterly valid silver measurements for one day
    When streaming to Gold
    Then there will be 96 points available in the Gold Table    
