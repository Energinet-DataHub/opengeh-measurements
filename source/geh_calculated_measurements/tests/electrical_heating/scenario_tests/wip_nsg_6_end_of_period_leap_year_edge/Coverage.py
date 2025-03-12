"""
## PURPOSE
The purpose is to detection of whether a period is a "leap period" or "non-leap period" (which affects how we calculate daily limits).

## DESIGN CONSIDERATIONS
CASE 4
                                    |2024       |SMRD 2                        | 01.07 1st Period change                    |2025 | 01.15 Period change |SMRD 2     Today|
                   Nov 2023_________|___________|______________________________|__________________________________________________|_____________________|_________________
Periods:                |_______________________|______________________________|__________________________________________________|_____________________|_________________...
Days in period:         | 62 days (non-leap)    | 151 days (leap)              | 199 days (leap)                                  | 16 days (leap)      | N/A
Measurement data:       +                      ++                             ++                                                 ++                    ++       +

NSG6 type:
- D15/End:              |_______________________________________________________________________________________________________________________________|
- D15/Up to End:                                                                                                                                        |_________________
"""
