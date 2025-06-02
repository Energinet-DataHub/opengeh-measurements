namespace Energinet.DataHub.Measurements.Domain;

[Obsolete("Use MeasurementAggregationByYear instead.")]
public record MeasurementAggregationByYearV4(
    int Year,
    decimal? Quantity,
    Quality Quality,
    Unit Unit);
