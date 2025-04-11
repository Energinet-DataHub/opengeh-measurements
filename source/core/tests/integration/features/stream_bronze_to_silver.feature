Feature: Streaming from Bronze to Silver

  Scenario: Processing submitted transactions to Silver
    Given valid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then measurements are available in the silver measurements table

  Scenario: Processing duplicated submitted transactions to Silver
    Given duplicated valid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then measurements are available in the silver measurements table

  Scenario: Streaming duplicate rows to Silver ensures only one row exists
    Given valid submitted transactions inserted into the silver measurements table and the same submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then measurements are available in the silver measurements table

  Scenario: Processing invalid submitted transactions
    Given invalid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then submitted transaction is persisted into the invalid bronze submitted transaction table

  Scenario Outline: Processing submitted transaction with unspecified <field>
    Given submitted transaction where the <field> has value <value>
    When streaming submitted transactions to the Silver layer
    Then submitted transaction is persisted into the bronze quarantine table and are not available in the silver measurements table
  Examples:
    | field               | value            |
    | resolution          | R_UNSPECIFIED    |
    | metering_point_type | MPT_UNSPECIFIED  |
    | unit                | U_UNSPECIFIED    |
    | orchestration_type  | OT_UNSPECIFIED   |

Scenario Outline: Processing submitted transaction with valid <field>
    Given submitted transaction where the <field> has value <value>
    When streaming submitted transactions to the Silver layer
    Then measurements are available in the silver measurements table

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
    | unit                | U_KWH                          |
    | unit                | U_KW                           |
    | unit                | U_MWH                          |
    | unit                | U_TONNE                        |
    | unit                | U_KVARH                        |

  Scenario Outline: Processing submitted transaction point with valid quality
    Given submitted transaction points where the quality has value <quality>
    When streaming submitted transactions to the Silver layer
    Then measurements are available in the silver measurements table

    Examples:
    | quality |
    | Q_MISSING     |
    | Q_ESTIMATED   |
    | Q_MEASURED    |
    | Q_CALCULATED  |
