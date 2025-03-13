"""
## PURPOSE
The purpose is to test scenarios where limit is not 4000

## DESIGN CONSIDERATIONS

CASE 1
                    ________|2024__________|2025___
Consumption start/end:       |------------------...
Measurement data:         +   +              +
Periods:                |___|______________|____...

CASE 2 - gap across two years
                    ________|2024__________________
Consumption start/end:  |--------------|
Measurement data:         +   +
Periods:                |___|__________|

CASE 3 - period of 1 day
                    ________|2024__________________
Consumption start/end:         ||
Measurement data:              +
Periods:                       ||

CASE 4
                    __|2024_______________
Consumption start/end:       |---------...
Measurement data:    +++++++++++++++++++++
Periods:                     |____________

CASE 5 - gap in same year
                    |2024        |2025
Parent start/end:   |------|  |--------|
Child 1 start/end:  |------|
Child 2 start/end:            |--------|
Measurement data:     +  +  +  +  +  +  +
Added to child:       *  *     *  *  *

## CASES TESTED
"""
Base consumption metering point behaviour
"GIVEN (total period consumption < period limit) WHEN (daily consumption + total period consumption =< period limit) THEN daily consumption in full should be added to consumption metering point for the day"
"GIVEN (total period consumption < period limit) WHEN (daily consumption + total period consumption > period limit) THEN (period limit-total period consumption) should be added to consumption metering point for the day"
"WHEN (total period consumption => period limit) THEN 0 should be added to consumption metering point for the day"
"GIVEN period = 1 year WHEN there is no end-of-period THEN base period limit = 4000"

Leap year
"GIVEN the period less than 1 year WHEN the period is a non-leap-year period THEN base period limit = (# period days/365 * 4000)"
"GIVEN the period less than 1 year WHEN the period is a leap-year period THEN base period limit = (# period days/366 * 4000)"


"GIVEN there is no active consumption metering point OR there is no active child electrical heating metering point WHEN there is measurement data for the day THEN no energy is added to an electrical heating child metering point for the day"
