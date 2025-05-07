using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a period.
/// </summary>
/// <param name="From">First observation time included in aggregation.</param>
/// <param name="To">Last observation time included in aggregation.</param>
/// <param name="Quantity">Quantity of aggregated measurement.</param>
/// <param name="Quality">Value of lowest quality among measurements in aggregation.</param>
/// <param name="Unit">Value of lowest unit among measurements in aggregation.</param>
public sealed record MeasurementAggregationByPeriodDto(Instant From, Instant To, decimal? Quantity, Quality Quality, Unit Unit);
