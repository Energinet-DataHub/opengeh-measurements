Feature: Transmission of measurements

Scenario: Processing measurement transaction
  Given a valid measurement transaction
  When the measurement transaction is enqueued in the Event Hub
  Then the measurement transaction is available in the Gold Layer
  And an acknowledgement is sent to the Event Hub