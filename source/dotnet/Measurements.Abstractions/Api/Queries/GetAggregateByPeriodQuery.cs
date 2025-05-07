using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated for a period for a metering point.
/// </summary>
public sealed record GetAggregateByPeriodQuery(string MeteringPointId, Instant From, Instant To);
