namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByYear(
    Year Year,
    decimal Quantity,
    Quality Quality,
    Unit Unit);
