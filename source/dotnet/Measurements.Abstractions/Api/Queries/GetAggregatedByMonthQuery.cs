using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting aggregated measurements for a metering point in a month specified by year and month.
/// </summary>
public sealed record GetAggregatedByMonthQuery(string MeteringPointId, YearMonth YearMonth);
