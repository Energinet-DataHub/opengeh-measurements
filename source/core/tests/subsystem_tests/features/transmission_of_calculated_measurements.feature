Feature: Transmission of calculated measurements

Scenario: Processing calculated measurements
  Given valid calculated measurements
  When inserted into the calculated measurements table
  Then the calculated measurements are avaiable in the Gold Layer