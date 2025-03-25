UPDATE {silver_database}.{silver_measurements_table}
SET metering_point_type = CASE
        WHEN metering_point_type == 'D01' THEN 'MPT_VE_PRODUCTION'
        WHEN metering_point_type == 'D02' THEN 'MPT_ANALYSIS'
        WHEN metering_point_type == 'D03' THEN 'MPT_NOT_USED'
        WHEN metering_point_type == 'D04' THEN 'MPT_SURPLUS_PRODUCTION_GROUP_6'
        WHEN metering_point_type == 'D05' THEN 'MPT_NET_PRODUCTION'
        WHEN metering_point_type == 'D06' THEN 'MPT_SUPPLY_TO_GRID'
        WHEN metering_point_type == 'D07' THEN 'MPT_CONSUMPTION_FROM_GRID'
        WHEN metering_point_type == 'D08' THEN 'MPT_WHOLESALE_SERVICES_INFORMATION'
        WHEN metering_point_type == 'D10' THEN 'MPT_NET_FROM_GRID'
        WHEN metering_point_type == 'D11' THEN 'MPT_NET_TO_GRID'
        WHEN metering_point_type == 'D12' THEN 'MPT_TOTAL_CONSUMPTION'
        WHEN metering_point_type == 'D13' THEN 'MPT_NET_LOSS_CORRECTION'
        WHEN metering_point_type == 'D14' THEN 'MPT_ELECTRICAL_HEATING'
        WHEN metering_point_type == 'D15' THEN 'MPT_NET_CONSUMPTION'
        WHEN metering_point_type == 'D17' THEN 'MPT_OTHER_CONSUMPTION'
        WHEN metering_point_type == 'D18' THEN 'MPT_OTHER_PRODUCTION'
        WHEN metering_point_type == 'D20' THEN 'MPT_EXCHANGE_REACTIVE_ENERGY'
        WHEN metering_point_type == 'E17' THEN 'MPT_CONSUMPTION'
        WHEN metering_point_type == 'E18' THEN 'MPT_PRODUCTION'
        WHEN metering_point_type == 'E20' THEN 'MPT_EXCHANGE'
        ELSE metering_point_type
    END
GO

UPDATE {gold_database}.{gold_measurements_table}
SET metering_point_type = CASE
        WHEN metering_point_type == 'D01' THEN 'MPT_VE_PRODUCTION'
        WHEN metering_point_type == 'D02' THEN 'MPT_ANALYSIS'
        WHEN metering_point_type == 'D03' THEN 'MPT_NOT_USED'
        WHEN metering_point_type == 'D04' THEN 'MPT_SURPLUS_PRODUCTION_GROUP_6'
        WHEN metering_point_type == 'D05' THEN 'MPT_NET_PRODUCTION'
        WHEN metering_point_type == 'D06' THEN 'MPT_SUPPLY_TO_GRID'
        WHEN metering_point_type == 'D07' THEN 'MPT_CONSUMPTION_FROM_GRID'
        WHEN metering_point_type == 'D08' THEN 'MPT_WHOLESALE_SERVICES_INFORMATION'
        WHEN metering_point_type == 'D10' THEN 'MPT_NET_FROM_GRID'
        WHEN metering_point_type == 'D11' THEN 'MPT_NET_TO_GRID'
        WHEN metering_point_type == 'D12' THEN 'MPT_TOTAL_CONSUMPTION'
        WHEN metering_point_type == 'D13' THEN 'MPT_NET_LOSS_CORRECTION'
        WHEN metering_point_type == 'D14' THEN 'MPT_ELECTRICAL_HEATING'
        WHEN metering_point_type == 'D15' THEN 'MPT_NET_CONSUMPTION'
        WHEN metering_point_type == 'D17' THEN 'MPT_OTHER_CONSUMPTION'
        WHEN metering_point_type == 'D18' THEN 'MPT_OTHER_PRODUCTION'
        WHEN metering_point_type == 'D20' THEN 'MPT_EXCHANGE_REACTIVE_ENERGY'
        WHEN metering_point_type == 'E17' THEN 'MPT_CONSUMPTION'
        WHEN metering_point_type == 'E18' THEN 'MPT_PRODUCTION'
        WHEN metering_point_type == 'E20' THEN 'MPT_EXCHANGE'
        ELSE metering_point_type
    END
