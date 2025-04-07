namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents a single measurement point.
/// </summary>
/// <param name="Quantity">Quantity of measurement.</param>
/// <param name="Quality">Quality of measurement.</param>
/// <param name="Unit">Unit of measure.</param>
/// <param name="Created">Timestamp defining when this point was created.</param>
public sealed record MeasurementPointDto(decimal Quantity, Quality Quality, Unit Unit, DateTimeOffset Created);
