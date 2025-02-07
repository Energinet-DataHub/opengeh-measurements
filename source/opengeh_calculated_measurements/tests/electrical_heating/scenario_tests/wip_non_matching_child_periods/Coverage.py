"""
## PURPOSE
The purpose is to test a scenario where parent period and child period do not overlap. This is really an invalid state, but it can happen i production, so we include a test for it.

## DESIGN CONSIDERATIONS

CASE 1
Parent start/end:   |-------------------...
Child start/end:    |---------|
Measurement data:   +++++++++++++++++++++++
Added to child:     ***********

CASE 2
Parent start/end:   |-------------------...
Child start/end:    nothing
Measurement data:   +++++++++++++++++++++++
Added to child:     nothing

## CASES TESTED
"""
