Feature: Streaming of calculated measurements

Scenario: Processing calculated measurements
  Given new valid calculated measurements
  When inserted into the calculated measurements table
  Then the calculated measurements are available in the Gold Layer

Scenario: this should fail and show the error in the test report as the steps exist
  Given a failing step

Scenario: this should fail and NOT show the error in the test report as the steps DOESNT exist
  Given a non-existing step