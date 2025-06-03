namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByDate(
    DateOnly Date,
    decimal? Quantity,
    IEnumerable<Quality> Qualities,
    Unit Unit,
    bool IsMissingValues,
    bool ContainsUpdatedValues);
