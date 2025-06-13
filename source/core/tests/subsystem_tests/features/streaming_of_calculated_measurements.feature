Feature: Streaming of calculated measurements

Scenario: Processing calculated measurements
  Given new valid calculated measurements
  When inserted into the calculated measurements table
  Then the calculated measurements are available in the Gold Layer
  And the calculated measurement transaction is available in the SAP Series Gold table

Scenario: Measurements transactions delivered to SAP Series Gold Table
  Given new valid calculated measurements
  When inserted into the calculated measurements table
