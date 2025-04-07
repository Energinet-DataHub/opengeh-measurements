Feature: Current View

  Scenario: Should not view null results
    Given a row where the column `<column>` is None
    When queryen the current view
    Then the query should not return the row

    Examples:
    | column              |
    | metering_point_type |
    | observation_time    |
    | quantity            |
    | quality             |
