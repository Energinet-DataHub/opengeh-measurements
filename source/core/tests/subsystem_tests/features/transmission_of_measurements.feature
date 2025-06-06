Feature: Transmission of measurements

  Background:
    Given a valid measurement transaction is enqueued in the Event Hub

  Scenario: Measurement transaction acknowledged after transmission
    Then an acknowledgement is sent to the Event Hub

  Scenario: Measurement transaction delivered to Gold Layer
    Then the measurement transaction is available in the Gold Layer

  Scenario: Measurements transaction delivered to SAP Series Gold Table
    Then the measurement transaction is available in the SAP Series Gold table