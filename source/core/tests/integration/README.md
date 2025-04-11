# Integration Tests

This repository uses pytest-bdd for integration testing with a behavior-driven approach. pytest-bdd integrates seamlessly with pytest and allows writing tests in a Gherkin syntax format (Given, When, Then).

Our primary focus is on testing our entry points, ensuring that they function correctly and interact properly with other components of the system.

## How We Use `pytest-bdd`

1. Define feature files
    - Write Gherkin syntax scenarios in .feature files inside the `features/` directory.

2. Create step definitions
    - Implement step functions in Python inside the `step_definitions/` directory, linking them to the Gherkin steps.

    - **Best Practice**: Reuse Given, When, and Then step definitions whenever possible to avoid duplicated code and improve maintainability.

## Auto-Generating Step Definitions

### Generate All Steps

You can use the following:

```md
pytest-bdd generate features/<insert_feature>.feature > ./../step_defitions/<new_test_file>.py
```

This will generate a new test file with all the steps. Be aware that the format is not exactly how we want it.

### Generate Missing Steps

You can use the following:

```md
pytest --generate-missing --feature <insert_feature>.feature ./../step_defitions/<insert_test_file>.py
```

to automatically generate step function template for undefined steps.

 The only outputs the missing steps in the Terminal, so you would have to copy-paste the code into the test file.

 ## Running Tests

 You can run the tests just like unit tests.
 