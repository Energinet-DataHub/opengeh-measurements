from coverage.all_test_cases import Cases

"""
## PURPOSE
The purpose is to test scenarios where limit is not 4000 with NSG2

## DESIGN CONSIDERATIONS
CASE 1
                                               |period change                         Today|
                    2023____________|2024______|______________________________|2025_________
Parent start/end:       |----------------------|-----------------------------------------...
Periods:                |___________|__________|______________________________|___________|
Days in period:         | 61 days   | 60 days  | 306 days                     | 90 days
Measurement data:       ++         +++        +++                            +++         +
NSG2 type:
- No D15/End:           |___________|__________|______________________________|
- No D15/No End:                                                              |___________|


## CASES TESTED
"""
