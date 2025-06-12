Feature:Sap_series_v1 View

  Scenario: View has expected schema
    When accessing the sap_series_v1 view
    Then the table schema should match the expected sap_series_v1 schema
