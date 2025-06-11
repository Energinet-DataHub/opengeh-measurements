Feature: Streaming Migrated to SAP series gold

  Scenario: Migrated transactions are ingested into the Gold SAP Series table
    Given valid migrated transactions inserted into the bronze migrated transactions table
    When streaming migrated transactions to the Gold layer
    Then the transaction is available in the gold SAP Series table