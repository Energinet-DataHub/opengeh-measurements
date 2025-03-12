namespace Energinet.DataHub.Measurements.Abstractions.Api.Dtos;

/// <summary>
/// Represents a series of measurements (points) for a single metering point.
/// </summary>
public sealed record MeasurementDto(string MeteringPointId, string Unit, IReadOnlyCollection<PointDto> Points);
