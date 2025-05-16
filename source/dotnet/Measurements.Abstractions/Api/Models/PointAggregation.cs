using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregation(
    Instant From,
    Instant To,
    decimal AggregatedQuantity,
    Quality Quality);
