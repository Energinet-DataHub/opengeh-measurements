using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregationGroup(
    Instant MinObservationTime,
    Instant MaxObservationTime,
    Resolution Resolution,
    List<PointAggregation> PointAggregations);
