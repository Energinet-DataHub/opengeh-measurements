namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated by month for a metering point specified by year.
/// </summary>
public sealed record GetAggregatedByYearQuery(string MeteringPointId, int Year);
