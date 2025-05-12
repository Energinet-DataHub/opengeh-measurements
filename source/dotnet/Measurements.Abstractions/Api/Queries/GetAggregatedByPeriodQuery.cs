using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting measurements aggregated by period for a metering point.
/// </summary>
public sealed record GetAggregateByPeriodQuery(List<string> MeteringPointIds, Instant From, Instant To, Aggregation Aggregation);
