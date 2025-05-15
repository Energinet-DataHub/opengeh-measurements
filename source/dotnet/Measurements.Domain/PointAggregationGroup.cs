using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record PointAggregationGroup(Instant From, Instant To, Resolution Resolution, List<PointAggregation> PointAggregations);
