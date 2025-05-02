namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated by year for a metering point.
/// </summary>
public sealed record GetAggregateByYearQuery(string MeteringPointId);
