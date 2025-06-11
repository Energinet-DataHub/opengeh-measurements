Feature: Current_sap_migrated_v1 Gold View


  Scenario: View has expected schema
    When accessing the current_sap_migrated_v1 gold view
    Then the table schema should match the expected current_sap_v1 schema

  Scenario: A gold measurement with orchestration type being migrated is included in current_sap_migrated_v1 view
    Given gold measurements with multiple transactions with one is of orcestration type migration
    When querying the current_sap_migrated_v1 gold view for that metering point
    Then the result should contain 1 rows