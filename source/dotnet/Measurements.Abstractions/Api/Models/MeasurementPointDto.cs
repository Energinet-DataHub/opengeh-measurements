namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a single measurement point.
/// </summary>
public sealed record MeasurementPointDto(DateTimeOffset ObservationTime, decimal Quantity, Quality Quality, Unit Unit, Resolution Resolution, DateTimeOffset Created);
