# Measurements.Client Release Notes

## Version 3.2.0

- Added method to get aggregated measurements for a year.
- MeasurementAggregationDto renamed to MeasurementAggregationByDateDto.

## Version 3.1.0

- Added RegistrationTime to MeasurementPointDto.

## Version 3.0.0

- Include historical values in forPeriod.

## Version 2.4.1

- Renamed Method and Query names to be more extendable.

## Version 2.4.0

- Unit added to MeasurementAggregationDto.

## Version 2.3.0

- ContainsUpdatedValues added to MeasurementAggregationDto.

## Version 2.2.0

- Added method to get aggregated measurements for a month.

## Version 2.1.0

- Add created timestamp to measurement point.

## Version 2.0.1

- Bugfix.

## Version 2.0.0

- Add authentication for API and implement authentication scheme for Measurements Client.

## Version 1.4.0

- Updated measurement points to only contain latest values.

## Version 1.3.0

- Added method to get measurements based on period.

## Version 1.2.0

- Updated Client interface.

## Version 1.1.4

- Bugfix. Don't reference Measurements from Client.

## Version 1.1.3

- Bugfix. Deserializing MeasurementDto.

## Version 1.1.2

- Serialize response with string enum values.

## Version 1.1.1

- Bugfix. Parse DateTimeOffset from Client as valid format.

## Version 1.1.0

- Converted from NodaTime Instant to DateTimeOffset.

## Version 1.0.0

- MVP release for exposing data for simple UI presentation.
