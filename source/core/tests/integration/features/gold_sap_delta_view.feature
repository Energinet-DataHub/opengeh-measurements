Feature:SAP_delta_v1 Gold View

  Scenario: View has expected schema
    When accessing the sap_delta_v1 gold view
    Then the table schema should match the expected sap_delta_v1 schema
