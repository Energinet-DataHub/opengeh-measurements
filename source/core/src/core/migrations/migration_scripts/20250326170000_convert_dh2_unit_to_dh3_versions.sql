UPDATE {silver_database}.{silver_measurements_table}
SET unit = CASE 
        WHEN unit == 'KWH'   THEN 'kWh'
        WHEN unit == 'MWH'   THEN 'MWh'
        WHEN unit == 'KVARH' THEN 'kVArh'
        WHEN unit == 'KW'    THEN 'kW'
        WHEN unit == 'T'     THEN 'Tonne' 
        ELSE unit
    END
GO

UPDATE {gold_database}.{gold_measurements}
SET unit = CASE 
        WHEN unit == 'KWH'   THEN 'kWh'
        WHEN unit == 'MWH'   THEN 'MWh'
        WHEN unit == 'KVARH' THEN 'kVArh'
        WHEN unit == 'KW'    THEN 'kW'
        WHEN unit == 'T'     THEN 'Tonne' 
        ELSE unit 
    END
