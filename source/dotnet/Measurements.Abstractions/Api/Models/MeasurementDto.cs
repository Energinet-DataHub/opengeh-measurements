namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a series of measurements (points) for a single metering point.
/// </summary>
public sealed record MeasurementDto(string MeteringPointId, Unit Unit, IReadOnlyCollection<PointDto> Points);
