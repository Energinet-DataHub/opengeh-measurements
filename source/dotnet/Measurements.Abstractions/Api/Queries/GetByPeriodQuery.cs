using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Queries;

/// <summary>
/// Query for getting current measurements for a metering point for a period.
/// </summary>
public sealed record GetByPeriodQuery(string MeteringPointId, Instant From, Instant To);
