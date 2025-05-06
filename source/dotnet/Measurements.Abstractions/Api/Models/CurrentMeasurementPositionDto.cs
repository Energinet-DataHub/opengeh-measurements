namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a collection of measurement points observed at the same position on a timeline.
/// </summary>
/// <param name="ObservationTime">Timestamp defining position on a timeline, i.e. '2025-03-12T03:15:00Z'.</param>
/// <param name="Quantity">Quantity of measurement.</param>
/// <param name="Quality">Quality of measurement.</param>
/// <param name="Unit">Unit of measurement.</param>
/// <param name="Resolution">Resolution of measurement.</param>
/// <param name="PersistedTime">Timestamp defining when this point was persisted.</param>
/// <param name="RegistrationTime">Timestamp defining when this point was registered.</param>

public sealed record CurrentMeasurementPositionDto(
    DateTimeOffset ObservationTime,
    decimal? Quantity,
    Quality Quality,
    Unit Unit,
    Resolution Resolution,
    DateTimeOffset PersistedTime,
    DateTimeOffset RegistrationTime);
