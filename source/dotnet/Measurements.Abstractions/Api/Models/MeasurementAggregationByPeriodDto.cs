namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a period.
/// </summary>
/// <param name="MeteringPointId">The ID of the metering point.</param>
/// <param name="PointAggregationGroups">A dictionary of point aggregation groups, where the key is the aggregation group ID and the value is the <see cref="PointAggregationGroup"/>.</param>
public sealed record MeasurementAggregationByPeriodDto(string MeteringPointId, Dictionary<string, PointAggregationGroup> PointAggregationGroups);
