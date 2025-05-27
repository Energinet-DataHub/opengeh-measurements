namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByDateV4(
    DateOnly Date,
    decimal? Quantity,
    Quality Quality,
    Unit Unit,
    bool IsMissingValues,
    bool ContainsUpdatedValues);
