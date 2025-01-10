from coverage.all_test_cases import Cases

"""
## PURPOSE
The purpose is to test scenarios where limit is not 4000

## DESIGN CONSIDERATIONS

CASE 1
                    ________|2024__________________
Parent start/end:       |-----------------------...
Child start/end:        |-----------------------...
Measurement data:         +   +
Periods:                |___|_____________________

CASE 2 - gap across two years
                    ________|2024__________________
Parent start/end:       |--------------|
Child start/end:        |--------------|

Measurement data:         +   +
Periods:                |____|__________|

CASE 3 - period of 1 day
                    ________|2024__________________
Parent start/end:              ||
Child start/end:               ||
Measurement data:               +
Periods:                       ||

CASE 4
                    __|2024_______________
Parent start/end:            |---------...
Child start/end:             |---------...
Measurement data:    +++++++++++++++++++++
Periods:                     |____________


## CASES TESTED
"""

