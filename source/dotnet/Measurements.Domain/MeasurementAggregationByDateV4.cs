namespace Energinet.DataHub.Measurements.Domain;

[Obsolete("Use MeasurementAggregationByDate instead. This will be removed in a future version.")]
public record MeasurementAggregationByDateV4(
    DateOnly Date,
    decimal? Quantity,
    Quality Quality,
    Unit Unit,
    bool IsMissingValues,
    bool ContainsUpdatedValues);
