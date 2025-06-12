Feature:Sap_series_v1 View

  Scenario: View has expected schema
    When accessing the sap_series_migrated_v1 view
    Then the table schema should match the expected sap_series_v1 schema


  Scenario: A gold measurement with orchestration type being migrated is included in sap_series_migrated_v1 view
    Given gold measurements with multiple transactions with one is of orcestration type migration
    When querying the sap_series_migrated_v1 gold view for that metering point
    Then the result should contain 1 rows
