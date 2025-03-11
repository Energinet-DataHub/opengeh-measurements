"""
## PURPOSE
The purpose is to test scenarios where limit is not 4000 with NSG6
Also to test settlement month != January

## DESIGN CONSIDERATIONS
CASE 2
                                    |2024                                 |Period change July 1st        |Settlement month 9   |2025      Today|
                   Nov 2023_________|_____________________________________|______________________________|_____________________|________________
Periods:                |_________________________________________________|______________________________|______________________________________
Days in period:         | 243 days                                        | 62 days                      | N/A
Measurement data:       ++                                               +++                            ++                    ++

NSG6 type:
- D15/End:              |________________________________________________________________________________|
- D15/Up to End:                                                                                         |______________________________________

CASE 3 (summer/winter time)
                                   |March 30th (1 day before summertime)        | October 28th (1 day after wintertime)
                        |2024______|____________________________________________|_______|2025
Parent start/end:                  |--------------------------------------------|
Days in period:                    | 213 days                                   |
Measurement data:                  +++                                        +++

## CASES TESTED
"""
