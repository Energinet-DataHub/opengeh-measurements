using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated by day for a metering point specified by year and month.
/// </summary>
public sealed record GetAggregatedByMonthQuery(string MeteringPointId, YearMonth YearMonth);
