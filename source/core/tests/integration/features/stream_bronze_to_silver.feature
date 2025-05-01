Feature: Streaming from Bronze to Silver

  Scenario: Processing submitted transactions to Silver
    Given valid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then 1 measurements row(s) are available in the silver measurements table

  Scenario: Processing duplicated submitted transactions to Silver
    Given duplicated valid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then 1 measurements row(s) are available in the silver measurements table

  Scenario: Streaming duplicate rows to Silver ensures only one row exists
    Given valid submitted transactions inserted into the silver measurements table and the same submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then 1 measurements row(s) are available in the silver measurements table

  Scenario: Processing invalid submitted transactions
    Given invalid submitted transactions inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then submitted transaction is persisted into the invalid bronze submitted transaction table

  Scenario: Processing submitted transactions with unknown version
    Given submitted transactions with unknown version inserted into the bronze submitted table
    When streaming submitted transactions to the Silver layer
    Then submitted transaction is persisted into the invalid bronze submitted transaction table    

  Scenario Outline: Processing submitted transaction with unspecified <field>
    Given submitted transaction where the <field> has value <value>
    When streaming submitted transactions to the Silver layer
    Then submitted transaction is persisted into the bronze quarantine table and are not available in the silver measurements table
  Examples:
    | field               | meaning         | value |
    | resolution          | R_UNSPECIFIED   | 0     |
    | metering_point_type | MPT_UNSPECIFIED | 0     |
    | unit                | U_UNSPECIFIED   | 0     |
    | orchestration_type  | OT_UNSPECIFIED  | 0     |

Scenario Outline: Processing submitted transaction with valid <field>
    Given submitted transaction where the <field> has value <value>
    When streaming submitted transactions to the Silver layer
    Then 1 measurements row(s) are available in the silver measurements table

  Examples:
    | field               | meaning                            | value |
    | metering_point_type | MPT_CONSUMPTION                    | 1     |
    | metering_point_type | MPT_PRODUCTION                     | 2     |
    | metering_point_type | MPT_EXCHANGE                       | 3     |
    | metering_point_type | MPT_VE_PRODUCTION                  | 4     |
    | metering_point_type | MPT_ANALYSIS                       | 5     |
    | metering_point_type | MPT_NOT_USED                       | 6     |
    | metering_point_type | MPT_SURPLUS_PRODUCTION_GROUP_6     | 7     |
    | metering_point_type | MPT_NET_PRODUCTION                 | 8     |
    | metering_point_type | MPT_SUPPLY_TO_GRID                 | 9     |
    | metering_point_type | MPT_CONSUMPTION_FROM_GRID          | 10    |
    | metering_point_type | MPT_WHOLESALE_SERVICES_INFORMATION | 11    |
    | metering_point_type | MPT_OWN_PRODUCTION                 | 1     |
    | metering_point_type | MPT_NET_FROM_GRID                  | 1     |
    | metering_point_type | MPT_NET_TO_GRID                    | 1     |
    | metering_point_type | MPT_TOTAL_CONSUMPTION              | 1     |
    | metering_point_type | MPT_NET_LOSS_CORRECTION            | 1     |
    | metering_point_type | MPT_NET_CONSUMPTION                | 1     |
    | metering_point_type | MPT_ELECTRICAL_HEATING             | 1     |
    | metering_point_type | MPT_OTHER_CONSUMPTION              | 1     |
    | metering_point_type | MPT_OTHER_PRODUCTION               | 2     |
    | metering_point_type | MPT_CAPACITY_SETTLEMENT            | 2     |
    | metering_point_type | MPT_EXCHANGE_REACTIVE_ENERGY       | 2     |
    | metering_point_type | MPT_COLLECTIVE_NET_PRODUCTION      | 2     |
    | metering_point_type | MPT_COLLECTIVE_NET_CONSUMPTION     | 2     |
    | metering_point_type | MPT_INTERNAL_USE                   | 2     |
    | resolution          | R_PT15M                            | 1     |
    | resolution          | R_PT1H                             | 2     |
    | resolution          | R_P1M                              | 3     |
    | orchestration_type  | OT_SUBMITTED_MEASURE_DATA          | 1     |
    | unit                | U_KWH                              | 1     |
    | unit                | U_KW                               | 2     |
    | unit                | U_MWH                              | 4     |
    | unit                | U_TONNE                            | 5     |

  Scenario Outline: Processing submitted transaction point with valid quality
    Given submitted transaction points where the quality has value <value> (<quality>)
    When streaming submitted transactions to the Silver layer
    Then 1 measurements row(s) are available in the silver measurements table

    Examples:
    | quality      | value |
    | Q_MISSING    | 1     |
    | Q_ESTIMATED  | 2     |
    | Q_MEASURED   | 3     |
    | Q_CALCULATED | 4     |
