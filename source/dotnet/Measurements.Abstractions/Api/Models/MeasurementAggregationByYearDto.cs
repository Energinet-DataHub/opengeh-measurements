namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a year.
/// </summary>
/// <param name="Year">The year of aggregation.</param>
/// <param name="Quantity">Quantity of aggregated measurement.</param>
/// <param name="Quality">Value of lowest quality among measurements in aggregation.</param>
/// <param name="Unit">Value of lowest unit among measurements in aggregation.</param>
public sealed record MeasurementAggregationByYearDto(int Year, decimal? Quantity, Quality Quality, Unit Unit);
