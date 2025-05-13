using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record PointAggregation(
    Instant MinObservationTime,
    Instant MaxObservationTime,
    decimal AggregatedQuantity,
    Quality Quality);
