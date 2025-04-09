Feature: Current View

  Scenario: The view must comply with the contract
    Given the current view version 1
    Then the schema should comply with the contract

  Scenario: The view should only show current measurements
    Given measurements for same metering point id and observation_time
    Then the query returns only the current measurements
  
  Scenario: Latest measurements are cancelled
    Given cancelled measurements
    Then the query should not return the measurements

  Scenario: View should not show metering point type with null
    Given measurements where metering point type is null
    Then the query should not return the measurements

  Scenario: View should not show observation time with null
    Given measurements where observation time is null
    Then the query should not return the measurements    

  Scenario: View should not show quantity with null
    Given measurements where quantity is null
    Then the query should not return the measurements    

  Scenario: View should not show quality with null
    Given measurements where quality is null
    Then the query should not return the measurements    
