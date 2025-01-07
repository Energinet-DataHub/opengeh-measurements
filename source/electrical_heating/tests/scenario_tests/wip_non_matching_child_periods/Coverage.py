from coverage.all_test_cases import Cases

"""
## PURPOSE
The purpose is to test withdrawal on a period before the transition to quarterly results.

## DESIGN CONSIDERATIONS

CASE 1
Parent:    |----------------...
Child:              |-------...
Added:              ***********

CASE 2
Parent:             |-------...
Child:     |----------------...
Added:              ***********

CASE 3
Parent:    |-------------------------------------------|
Child:          |--------|                  |--------|
Added:          **********                  **********

CASE 4
Parent:         |--------|                  |--------|
Child:     |-------------------------------------------|
Added:          **********                  **********

CASE 5
Parent:         |--------|                  |--------|
Child:              |-----------------------------|
Added:              ******                  *******

CASE 6
Parent:             |-----------------------------|
Child:          |--------|                  |--------|
Added:              ******                  *******

## CASES TESTED
"""

