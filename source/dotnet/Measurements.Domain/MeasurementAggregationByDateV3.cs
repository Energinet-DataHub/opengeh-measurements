namespace Energinet.DataHub.Measurements.Domain;

[Obsolete("MeasurementAggregationByDateV3 is deprecated. Use MeasurementAggregationByDate instead.")]
public record MeasurementAggregationByDateV3(
    DateOnly Date,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    bool MissingValues,
    bool ContainsUpdatedValues);
