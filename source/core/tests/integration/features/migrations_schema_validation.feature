Feature: Schema validation for migrated tables after migration jobs

  Scenario: Silver measurements table is created with correct schema
    When accessing the silver measurements table
    Then the table schema should match the expected silver measurements schema

  Scenario: Gold measurements table is created with correct schema
    When accessing the gold measurements table
    Then the table schema should match the expected gold measurements schema

  Scenario: Gold measurements SAP series table is created with correct schema
    When accessing the gold measurements SAP series table
    Then the table schema should match the expected gold measurements SAP series schema    

  Scenario: Bronze migrated transactions table is created with correct schema
    When accessing the bronze migrated transactions table
    Then the table schema should match the expected bronze migrated transactions schema

  Scenario: Bronze submitted transactions table is created with correct schema
    When accessing the bronze submitted transactions table
    Then the table schema should match the expected bronze submitted transactions schema

  Scenario: Bronze invalid submitted transactions table is created with correct schema
    When accessing the bronze invalid submitted transactions table
    Then the table schema should match the expected bronze invalid submitted transactions schema

  Scenario: Bronze quarantined transactions table is created with correct schema
    When accessing the bronze submitted transactions quarantined table
    Then the table schema should match the expected bronze quarantined transactions schema

  Scenario: Silver measurements table enforces NOT NULL constraint on is_cancelled
    Given a row for the silver measurements table where is_cancelled is null
    When attempting to insert the row into the silver measurements table
    Then the insert should raise an exception
    And the number of rows in the silver measurements table should remain unchanged
 