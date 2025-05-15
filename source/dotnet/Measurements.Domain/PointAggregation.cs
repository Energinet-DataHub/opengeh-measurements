using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record PointAggregation(
    Instant From,
    Instant To,
    decimal AggregatedQuantity,
    Quality Quality);
