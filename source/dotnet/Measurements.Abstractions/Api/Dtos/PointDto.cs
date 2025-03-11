using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Dtos;

/// <summary>
/// Represents a single measurement point.
/// </summary>
public sealed record PointDto(Instant ObservationTime, decimal Quantity, string Quality);
