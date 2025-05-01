Feature: Appending quarantined submitted transactions

  Scenario: Appending quarantined submitted transactions
    Given quarantined submitted transactions
    When appending the quarantined submitted transactions to the bronze submitted transactions quarantined table
    Then the quarantined submitted transactions are available in the bronze submitted transactions quarantined table
