from coverage.all_test_cases import Cases

"""
## PURPOSE
The purpose is to test withdrawal on a period before the transition to quarterly results.

## DESIGN CONSIDERATIONS

CASE 1
Parent start/end:   |-------------------...
Child start/end:                |-------...
Measurement data:   +++++++++++++++++++++++
Added to child:                 ***********

CASE 2
Parent start/end:               |-------...
Child start/end:    |-------------------...
Measurement data:   +++++++++++++++++++++++
Added to child:                 ***********

CASE 3
Parent start/end:    |-------------------------------------------|
Child start/end:        |--------|                  |--------|
Measurement data:    +++++++++++++++++++++++++++++++++++++++++++++
Added to child:         **********                  **********

CASE 4
Parent start/end:       |--------|                  |--------|
Child start/end:     |-------------------------------------------|
Measurement data:    +++++++++++++++++++++++++++++++++++++++++++++
Added to child:         **********                  **********

CASE 5
Parent start/end:       |--------|                  |--------|
Child start/end:            |-----------------------------|
Measurement data:       +++++++++++++++++++++++++++++++++++++++++++++
Added to child:             *****                   *******

CASE 6
Parent start/end:           |-----------------------------|
Child start/end:        |--------|                  |--------|
Measurement data:       +++++++++++++++++++++++++++++++++++++++++++++
Added to child:             *****                   *******

## CASES TESTED
"""

