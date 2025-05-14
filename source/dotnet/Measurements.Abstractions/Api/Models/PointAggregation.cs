using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregation(
    Instant MinObservationTime,
    Instant MaxObservationTime,
    decimal AggregatedQuantity,
    Quality Quality);
