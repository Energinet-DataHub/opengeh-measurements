using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Requests;

public record GetAggregatedByPeriodRequest(string MeteringPointIds, Instant DateFrom, Instant DateTo, Aggregation Aggregation);
