namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a single date.
/// </summary>
/// <param name="Date">Date of aggregation.</param>
/// <param name="Quantity">Quantity of aggregated measurement.</param>
/// <param name="Quality">Value of lowest quality among measurements in aggregation.</param>
/// <param name="Unit">Unit of aggregated measurement.</param>
/// <param name="MissingValues">Indicates whether aggregate contains missing values during day of aggregation.</param>
/// <param name="ContainsUpdatedValues">Indicates whether any measurements in aggregate has been updated since creation.</param>
public sealed record MeasurementAggregationDto(DateOnly Date, decimal Quantity, Quality Quality, Unit Unit, bool MissingValues, bool ContainsUpdatedValues);
