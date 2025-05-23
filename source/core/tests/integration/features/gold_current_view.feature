Feature: Current_v1 Gold View

  Scenario: View has expected schema
    When accessing the current_v1 gold view
    Then the table schema should match the expected current_v1 schema

  Scenario: View returns only the latest active (not cancelled) measurement
    Given gold measurements with multiple transactions for the same metering point where only the latest is active
    When querying the current_v1 gold view for that metering point and expected quantity
    Then the result should contain 1 row with the expected quantity

  Scenario: View returns no result when the latest measurement is cancelled
    Given gold measurements with multiple transactions for the same metering point where the latest is cancelled
    When querying the current_v1 gold view for that metering point
    Then the result should contain 0 rows

  Scenario Outline: A gold measurement with null <column> is excluded from current_v1 view
    Given a gold measurement where <column> is null
    When querying the current_v1 gold view for that metering point
    Then the result should contain 0 rows

    Examples:
      | column              |
      | metering_point_type |
      | observation_time    |
      | quality             |
