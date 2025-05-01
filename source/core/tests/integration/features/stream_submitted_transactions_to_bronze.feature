Feature: Stream submitted transaction to bronze table

Scenario: Processing Submitted Transactions
  Given a submitted transaction on the event hub
  When ingesting submitted transaction to bronze
  Then one submitted transaction is available in the submitted bronze table