Feature: Streaming from Bronze to Silver

  Scenario: Processing submitted measurements to Silver
    Given new valid submitted measurements inserted into the bronze submitted table
    When streaming submitted transactions to the Silver Layer
    Then the measurements are available in the silver measurements table

  Scenario: Processing invalid submitted measurements
    Given invalid submitted measurements inserted into the bronze submitted table
    When streaming submitted transactions to the Silver Layer
    Then the measurements are persisted into the invalid bronze submitted transaction table
    And are not available in the silver measurements table

  Scenario: Processing submitted transaction with unspecified resolution
    Given a submitted transaction with unspecified resolution
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table
    And are not available in the silver measurements table

  Scenario Outline: Processing unspecified values should be quarantined
    Given a row where the column <column> value is Unspecified
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table
    And are not available in the silver measurements table

    Examples:
    | column              |
    | orchestration_type  |
    | metering_point_type |
    | resolution          |
    | unit                |
