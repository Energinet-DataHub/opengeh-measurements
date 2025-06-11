# Measurements.Client Release Notes

## Version 8.4.2

- Refactor B2C authentication.

## Version 8.4.1

- Bugfix.

## Version 8.4.0

- Remove quality and missing values indicator for monthly and yearly aggregates.

## Version 8.3.0

- CreateAuthorizationHeaderValueAsync is added to IAuthorizationHeaderProvider.

## Version 8.2.0

- Converted quality for daily aggregate to a list of qualities present in the aggregate.

## Version 8.1.0

- Consumers can now pass a custom IAuthorizationHeaderProvider implementation when registering the Measurements Client, which can be used to enable B2C credentials.
- If no custom provider is specified, a default implementation using Azure Entra credentials will be used.

```csharp
      services.AddMeasurementsClient(options =>
   {
      options.AuthorizationHeaderProvider = new B2CAuthorizationHeaderProvider();
   });
```

## Version 8.0.0

- In `MeasurementAggregationByDate`, `MissingValues` is renamed to `IsMissingValues`
- In `PointAggregation`, `AggregatedQuantity` is renamed to `Quantity`
- `Quantity` is changed to nullable decimal (`decimal?`)

## Version 7.1.1

- fix link to documentation in package README
- add `Measurements.Client` documentation

## Version 7.0.0

- Added two new dummy methods to the MeasurementsClient, `GetCurrentByPeriodAsync` and `GetAggregateByPeriodAsync`.
- `Quantity` property of `MeasurementPointDto` is changed to a nullable `decimal`

## Version 6.0.0

- Upgrade to dotnet version 9.0

## Version 5.0.0

- Added method to get aggregated measurements for all years.
- GetYearlyAggregateByMonthsQuery renamed to GetYearlyAggregateByMonthQuery.
- Unused properties of MeasurementAggregationByMonthDto removed.

## Version 4.0.0

- Aggregated Measurements endpoint routes renamed.

## Version 3.4.2

- Renaming of response types - no functional changes.

## Version 3.4.1

- Bugfix.

## Version 3.4.0

- Removed support for V1 and unversioned API endpoints.

## Version 3.3.0

- Added method to get aggregated measurements for a year.
- MeasurementAggregationDto renamed to MeasurementAggregationByDateDto.

## Version 3.2.0

- Minor API changes.

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
