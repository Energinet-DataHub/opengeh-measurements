namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a single measurement point.
/// </summary>
public sealed record PointDto(DateTimeOffset ObservationTime, decimal Quantity, Quality Quality);
