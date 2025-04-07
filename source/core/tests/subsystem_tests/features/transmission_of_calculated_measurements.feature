Feature: Streaming of calculated measurements

Scenario: Processing calculated measurements
  Given new valid calculated measurements
  When inserted into the calculated measurements table
  Then the calculated measurements are available in the Gold Layer
