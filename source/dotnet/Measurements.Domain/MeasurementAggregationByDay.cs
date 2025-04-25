namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByDay(
    DateOnly Date,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    bool MissingValues,
    bool ContainsUpdatedValues);
