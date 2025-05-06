namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByYear(
    int Year,
    decimal Quantity,
    Quality Quality,
    Unit Unit);
