UPDATE {silver_database}.{silver_measurements_table}
SET unit = CASE 
        WHEN unit == 'T'     THEN 'TONNE'
        ELSE unit
    END
GO

UPDATE {gold_database}.{gold_measurements}
SET unit = CASE 
        WHEN unit == 'T'     THEN 'TONNE'
        ELSE unit 
    END
