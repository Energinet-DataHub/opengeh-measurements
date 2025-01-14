from coverage.all_test_cases import Cases

"""
## PURPOSE
The purpose is to test gaps in periods with EH for the same metering point

## DESIGN CONSIDERATIONS

CASE 1 - gap in same year
                    |2024
Parent start/end:   |---------|     |---|   |---...
Child start/end:    |---------|     |---|   |---...
Measurement data:   +++++++++++++++++++++++********
Added to child:     ***********     *****   *******

## CASES TESTED
"""
