namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregation(DateOnly Date, decimal Quantity, Quality Quality, Unit Unit, bool MissingValues, bool ContainsUpdatedValues);
