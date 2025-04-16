namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public sealed record MeasurementAggregationDto(DateOnly Date, decimal Quantity, Quality Quality, Unit Unit, bool MissingValues, bool ContainsUpdatedValues);
