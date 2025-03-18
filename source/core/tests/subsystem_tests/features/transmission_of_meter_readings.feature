Feature: Transmission of meter readings

Scenario: Processing measurement transaction
  Given a valid measurement transaction
  When the measurement transaction is enqueued in the Event Hub
  Then an acknowledgement is sent to the Event Hub