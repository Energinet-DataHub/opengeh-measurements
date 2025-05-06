using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated by date for a metering point specified by year and month.
/// </summary>
public sealed record GetAggregatedByDateQuery(string MeteringPointId, YearMonth YearMonth);
