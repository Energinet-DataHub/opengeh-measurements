Feature: Appending quarantined submitted transactions

  Scenario: Appending quarantined submitted transactions
    Given submitted transactions are quarantined
    When appending them to the bronze submitted transactions quarantined table
    Then the transactions are available in the bronze submitted transactions quarantined table
