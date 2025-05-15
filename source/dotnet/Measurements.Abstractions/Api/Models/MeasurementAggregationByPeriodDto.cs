namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

/// <summary>
/// Represents measurement aggregation for a period.
/// </summary>
/// <param name="MeteringPoint">An object containing the ID of the metering point.</param>
/// <param name="PointAggregationGroups">A dictionary of point aggregation groups, where the key is the aggregation group ID and the value is the <see cref="PointAggregationGroup"/>.</param>
public sealed record MeasurementAggregationByPeriodDto(MeteringPoint MeteringPoint, Dictionary<string, PointAggregationGroup> PointAggregationGroups);
