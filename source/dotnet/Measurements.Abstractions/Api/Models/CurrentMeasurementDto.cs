namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a collection of measurements for a given metering point.
/// </summary>
/// <param name="MeasurementPositions">A collection of measurement positions constituting a continuous timeline.</param>
public sealed record CurrentMeasurementDto(IEnumerable<CurrentMeasurementPositionDto> MeasurementPositions);
