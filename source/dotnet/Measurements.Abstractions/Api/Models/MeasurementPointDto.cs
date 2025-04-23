namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a single measurement point.
/// </summary>
/// <param name="Order">Order of priority. Order of 1 is the current value. 2+ are historical values.</param>
/// <param name="Quantity">Quantity of measurement.</param>
/// <param name="Quality">Quality of measurement.</param>
/// <param name="Unit">Unit of measurement.</param>
/// <param name="Resolution">Resolution of measurement.</param>
/// <param name="CreatedInDataHub">Timestamp defining when this point was created in DataHub.</param>
/// <param name="CreatedByGridAccessProvider">Timestamp defining when this point was created by Grid Access Provider.</param>
public sealed record MeasurementPointDto(
    int Order,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    Resolution Resolution,
    DateTimeOffset CreatedInDataHub,
    DateTimeOffset CreatedByGridAccessProvider);
