Feature: Handling unknown submitted transactions

  Scenario: Handling unknown submitted transactions
    Given invalid submitted transactions with different topics are received
    When handling the invalid submitted transactions
    Then only expected topics are persisted in the bronze invalid submitted transactions table