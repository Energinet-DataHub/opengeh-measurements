using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a month.
/// </summary>
/// <param name="YearMonth">The month of aggregation.</param>
/// <param name="Quantity">Quantity of aggregated measurement.</param>
/// <param name="Quality">Value of lowest quality among measurements in aggregation.</param>
/// <param name="Unit">Unit of aggregated measurement.</param>
public sealed record MeasurementAggregationByMonthDto(YearMonth YearMonth, decimal Quantity, Quality Quality, Unit Unit);
