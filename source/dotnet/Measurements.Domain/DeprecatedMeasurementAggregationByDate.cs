namespace Energinet.DataHub.Measurements.Domain;

public record DeprecatedMeasurementAggregationByDate(
    DateOnly Date,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    bool MissingValues,
    bool ContainsUpdatedValues);
