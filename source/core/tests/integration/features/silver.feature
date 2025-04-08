Feature: Streaming from Bronze to Silver

  Scenario: Processing submitted measurements to Silver
    Given new valid submitted measurements inserted into the bronze submitted table
    When streaming submitted transactions to the Silver Layer
    Then the measurements are available in the silver measurements table

  Scenario: Processing invalid submitted measurements
    Given invalid submitted measurements inserted into the bronze submitted table
    When streaming submitted transactions to the Silver Layer
    Then the measurements are persisted into the invalid bronze submitted transaction table
    And are not available in the silver measurements table

  Scenario: Processing submitted transaction with unspecified resolution
    Given a submitted transaction with unspecified resolution
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table
    And are not available in the silver measurements table

  Scenario Outline: Processing unspecified values should be quarantined
    Given a row where the column <column> value is Unspecified
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table
    And are not available in the silver measurements table

    Examples:
    | column              |
    | orchestration_type  |
    | metering_point_type |
    | resolution          |
    | unit                |

  Scenario Outline: Processing submitted transaction with valid metering point type
    Given measurements where the metering point type has value <metering_point_type>
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the silver measurements table
    And are not quarantined in the bronze quarantine table

    Examples:
    | metering_point_type                 |
    | MPT_ANALYSIS                        |
    | MPT_CAPACITY_SETTLEMENT             |
    | MPT_COLLECTIVE_NET_CONSUMPTION      |
    | MPT_COLLECTIVE_NET_PRODUCTION       |
    | MPT_CONSUMPTION                     |
    | MPT_CONSUMPTION_FROM_GRID           |
    | MPT_CONSUMPTION                     |
    | MPT_ELECTRICAL_HEATING              |
    | MPT_EXCHANGE                        |
    | MPT_EXCHANGE_REACTIVE_ENERGY        |
    | MPT_INTERNAL_USE                    |
    | MPT_NET_CONSUMPTION                 |
    | MPT_NET_FROM_GRID                   |
    | MPT_NET_LOSS_CORRECTION             |
    | MPT_NET_PRODUCTION                  |
    | MPT_NET_TO_GRID                     |
    | MPT_NOT_USED                        |
    | MPT_OTHER_CONSUMPTION               |
    | MPT_OTHER_PRODUCTION                |
    | MPT_OWN_PRODUCTION                  |
    | MPT_PRODUCTION                      |
    | MPT_SUPPLY_TO_GRID                  |
    | MPT_SURPLUS_PRODUCTION_GROUP_6      |
    | MPT_TOTAL_CONSUMPTION               |
    | MPT_VE_PRODUCTION                   |
    | MPT_WHOLESALE_SERVICES_INFORMATION  |

  Scenario Outline: Processing submitted transaction with valid resolution
    Given measurements where the resolution has value <resolution>
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the silver measurements table
    And are not quarantined in the bronze quarantine table

    Examples:
    | resolution |
    | R_PT15M    |
    | R_PT1H     |

  Scenario: Processing submitted transaction with valid orchestration type
    Given measurements where the orchestration type has value `OT_SUBMITTED_MEASURE_DATA`
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the silver measurements table
    And are not quarantined in the bronze quarantine table

  Scenario Outline: Processing submitted transaction with valid quality
    Given measurements where the quality has value <quality>
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the silver measurements table
    And are not quarantined in the bronze quarantine table

    Examples:
    | quality       |
    | Q_MISSING     |
    | Q_ESTIMATED   |
    | Q_MEASURED    |
    | Q_CALCULATED  |

  Scenario Outline: Processing submitted transaction with valid unit
    Given measurements where the unit has value <unit>
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the silver measurements table
    And are not quarantined in the bronze quarantine table

    Examples:
    | unit       |
    | U_KWH      |
    | U_KW       |
    | U_MW       |
    | U_MWH      |
    | U_TONN     |
    | U_KVAR     |
    | U_MVAR     |