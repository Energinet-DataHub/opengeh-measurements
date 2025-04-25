Feature: Stream migrated transaction into Measurements gold

Scenario: Processing migrated transaction
  Given a new valid migrated transaction
  When inserted into the Migration silver table
  Then the migrated transaction is available in the Gold layer
