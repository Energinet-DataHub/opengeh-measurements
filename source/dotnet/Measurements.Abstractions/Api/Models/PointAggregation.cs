namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record PointAggregation(
    long MinObservationTime,
    long MaxObservationTime,
    decimal AggregatedQuantity,
    Quality Quality);
