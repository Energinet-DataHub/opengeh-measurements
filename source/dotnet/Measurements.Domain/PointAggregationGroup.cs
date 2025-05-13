using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record PointAggregationGroup(Instant MinObservationTime, Instant MaxObservationTime, Resolution Resolution, List<PointAggregation> PointAggregations);
