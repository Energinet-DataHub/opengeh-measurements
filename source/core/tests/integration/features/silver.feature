Feature: Streaming from Bronze to Silver

  Scenario: Processing submitted measurements to Silver
    Given new valid submitted measurements inserted into the bronze submitted table
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

  Scenario: Processing invalid submitted measurements
    Given invalid submitted measurements inserted into the bronze submitted table
    When streaming the submitted transaction to the Silver layer
    Then measurements are persisted into the invalid bronze submitted transaction table

  Scenario: Processing invalid submitted measurements
    Given submitted measurements with unknown version inserted into the bronze submitted table
    When streaming the submitted transaction to the Silver layer
    Then measurements are persisted into the invalid bronze submitted transaction table    

  Scenario: Processing submitted transaction with unspecified resolution
    Given a submitted transaction with unspecified resolution
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table and are not available in the silver measurements table

  Scenario: Processing submitted transaction with unspecified metering point type
    Given a submitted transaction with unspecified metering point type
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table and are not available in the silver measurements table

  Scenario: Processing submitted transaction with unspecified unit
    Given a submitted transaction with unspecified unit
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table and are not available in the silver measurements table

  Scenario: Processing submitted transaction with unspecified orchestration type
    Given a submitted transaction with unspecified orchestration type
    When streaming the submitted transaction to the Silver layer
    Then the transaction are persisted into the bronze quarantine table and are not available in the silver measurements table            

  Scenario Outline: Processing submitted transaction with valid metering point type
    Given measurements where the metering point type has value <metering_point_type>
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

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
    Then the measurements are available in the silver measurements table

    Examples:
    | resolution |
    | R_PT15M    |
    | R_PT1H     |

  Scenario: Processing submitted transaction with valid orchestration type
    Given measurements where the orchestration type has value `OT_SUBMITTED_MEASURE_DATA`
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

  Scenario Outline: Processing submitted transaction wih valid quality
    Given measurements where the quality has value <quality>
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

    Examples:
    | quality |
    | Q_MISSING     |
    | Q_ESTIMATED   |
    | Q_MEASURED    |
    | Q_CALCULATED  |

  Scenario Outline: Processing submitted transaction with valid unit
    Given measurements where the unit has value <unit>
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

    Examples:
    | unit       |
    | U_KWH      |
    | U_KW       |
    | U_MWH      |
    | U_TONNE     |
    | U_KVARH     |