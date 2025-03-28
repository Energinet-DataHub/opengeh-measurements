# Tests

The tests container contains 3 different types of tests which will be described here.

| **Aspect**        | Unit Tests               | Integration Tests         | Subsystem Tests               |
|-------------------|--------------------------|---------------------------|-------------------------------|
| **Scope**         | Single unit              | Component interactions    | Full environment              |
| **Mocks**         | All dependencies         | External systems only     | None (live resources)         |
| **Trigger**       | CI                       | CI                        | CD - Post-deployment (dev001) |

## Unit Tests

Unit tests are simple tests that ensure that the smallest parts of the application, such as individual functions or
 methods, work as expected in isolation.

This means that most of the logic is being mocked.

## Integration Tests

Integration tests verify that different modules or components of the application work together as intended.
They test the interaction between these components.

This means that we do create fictive tables that matches the ones in a deployed environment.

## Subsystem Tests

Subsystem tests are tests being run in an actual environment, in this case dev001. It is running every time
we do a deploymenet and if it fails, the deployment pipeline is blocked.

The subsystem tests are simple in its form, but it requires that all the streaming jobs are running in the environment.

The subsystem tests can be run locally towards dev002/003 - remember to deploy to the environment first to ensure that
code changes are deployed to the environment.

See [`Running Subsystem Tests Locally`](./subsystem_tests/README.md)
