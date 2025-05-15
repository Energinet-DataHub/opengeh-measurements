using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregationGroup(
    Instant From,
    Instant To,
    Resolution Resolution,
    List<PointAggregation> PointAggregations);
