Feature: Appending invalid and quarantined submitted transactions

  Scenario: Appending invalid submitted transactions
    Given invalid submitted transactions are created
    When appending them to the bronze invalid submitted transactions table
    Then the transactions are available in the bronze invalid submitted transactions table

  Scenario: Appending quarantined submitted transactions
    Given submitted transactions are quarantined
    When appending them to the bronze submitted transactions quarantined table
    Then the transactions are available in the bronze submitted transactions quarantined table
