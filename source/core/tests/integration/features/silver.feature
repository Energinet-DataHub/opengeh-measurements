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

Scenario Outline: Processing submitted transaction with valid <field>
    Given measurements where the <field> has value <value>
    When streaming the submitted transaction to the Silver layer
    Then the measurements are available in the silver measurements table

  Examples:
    | field               | value                          |
    | metering_point_type | MPT_ANALYSIS                   |
    | metering_point_type | MPT_CAPACITY_SETTLEMENT        |
    | metering_point_type | MPT_COLLECTIVE_NET_CONSUMPTION |
    | metering_point_type | MPT_COLLECTIVE_NET_PRODUCTION  |
    | metering_point_type | MPT_CONSUMPTION                |
    | metering_point_type | MPT_CONSUMPTION_FROM_GRID      |
    | metering_point_type | MPT_ELECTRICAL_HEATING         |
    | metering_point_type | MPT_EXCHANGE                   |
    | metering_point_type | MPT_EXCHANGE_REACTIVE_ENERGY   |
    | metering_point_type | MPT_INTERNAL_USE               |
    | metering_point_type | MPT_NET_CONSUMPTION            |
    | metering_point_type | MPT_NET_FROM_GRID              |
    | metering_point_type | MPT_NET_LOSS_CORRECTION        |
    | metering_point_type | MPT_NET_PRODUCTION             |
    | metering_point_type | MPT_NET_TO_GRID                |
    | metering_point_type | MPT_NOT_USED                   |
    | metering_point_type | MPT_OTHER_CONSUMPTION          |
    | metering_point_type | MPT_OTHER_PRODUCTION           |
    | metering_point_type | MPT_OWN_PRODUCTION             |
    | metering_point_type | MPT_PRODUCTION                 |
    | metering_point_type | MPT_SUPPLY_TO_GRID             |
    | metering_point_type | MPT_SURPLUS_PRODUCTION_GROUP_6 |
    | metering_point_type | MPT_TOTAL_CONSUMPTION          |
    | metering_point_type | MPT_VE_PRODUCTION              |
    | metering_point_type | MPT_WHOLESALE_SERVICES_INFORMATION |
    | resolution          | R_PT15M                        |
    | resolution          | R_PT1H                         |
    | orchestration_type  | OT_SUBMITTED_MEASURE_DATA      |
    | quality             | Q_MISSING                      |
    | quality             | Q_ESTIMATED                    |
    | quality             | Q_MEASURED                     |
    | quality             | Q_CALCULATED                   |
    | unit                | U_KWH                          |
    | unit                | U_KW                           |
    | unit                | U_MWH                          |
    | unit                | U_TONNE                        |
    | unit                | U_KVARH                        |
