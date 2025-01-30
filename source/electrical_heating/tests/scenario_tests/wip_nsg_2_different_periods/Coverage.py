
"""
## PURPOSE
The purpose is to test scenarios where limit is not 4000 with NSG2

## DESIGN CONSIDERATIONS
CASE 1
                                               |period change in april                 Today|
                   Nov 2023_________|2024______|______________________________|2025_________
Parent start/end:       |----------------------|-----------------------------------------...
Periods:                |___________|__________|______________________________|__________
Days in period:         | 61 days   | 91 days  | 2xx days                     |
Measurement data:       ++         +++        +++                            +++         +
NSG2 type:
- No D15/End:           |___________|__________|______________________________|
- No D15/Up to End:                                                           |___________


## CASES TESTED
"""
