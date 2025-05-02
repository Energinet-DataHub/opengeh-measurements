Feature: Stream migrated transaction into Measurements gold

Scenario: Processing migrated transaction
  Given a new valid migrated transaction inserted into the Migration Silver table
  When streaming from Migration silver to Measurements gold
  Then the migrated transaction is available in the Gold layer
