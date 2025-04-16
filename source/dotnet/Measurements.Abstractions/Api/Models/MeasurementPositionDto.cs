namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a collection of measurement points observed at the same position on a timeline.
/// </summary>
/// <param name="Index">Index of position on requested timeline. I.e. 1-24 for daily measurements with a hourly resolution</param>
/// <param name="ObservationTime">Timestamp defining position on a timeline, i.e. '2025-03-12T03:15:00Z'.</param>
/// <param name="MeasurementPoints">Points of measurements constituting current and historical measurements.</param>
public sealed record MeasurementPositionDto(int Index, DateTimeOffset ObservationTime, IEnumerable<MeasurementPointDto> MeasurementPoints);
