"""
## PURPOSE
The purpose is to test gaps in periods with EH for the same metering point.

## DESIGN CONSIDERATIONS

CASE 1 - gap in same year
                    |2024        |2025
Parent start/end:   |---------|         |---|   |---...
Child 1 start/end:  |---------|
Child 2 start/end:                      |---|
Child 3 start/end:                              |---...
Measurement data:   +++++++++++++++++++++++*********
Added to child:     ***********         *****   ****

## CASES TESTED
"""
