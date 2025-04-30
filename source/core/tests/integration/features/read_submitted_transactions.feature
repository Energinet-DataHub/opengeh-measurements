Feature: Reading submitted transactions

  Scenario: Reading submitted transactions
    Given submitted transactions are available
    When reading submitted transactions
    Then the result is a streaming dataframe
