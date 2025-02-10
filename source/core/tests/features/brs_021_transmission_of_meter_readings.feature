Feature: BRS-021: transmission of meter readings

    As a system handling meter readings,
    I want to receive, store, and acknowledge meter readings,
    So that data integrity and process completion are ensured.

    Scenario: Processing meter readings
        Given a valid meter reading has been enqueued in the Event Hub
        When the Measurement system processes the queued meter reading
        Then the meter reading is successfully recorded in Measurements
        And an acknowledgment is sent back to the Event Hub